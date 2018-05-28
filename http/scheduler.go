package main

import (
	"bytes"
	"context"
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/michaelbironneau/analyst/http/models"
	"os/exec"
	"sync"
	"time"
)

type invocation struct {
	running int64
	cancel context.CancelFunc
	lastExec time.Time
}

type Scheduler struct {
	sync.RWMutex
	ctx              context.Context
	InvocationOutput chan string //
	DB               *gorm.DB
	logger           echo.Logger
	tasks            map[uint]*invocation
}

func NewScheduler(db *gorm.DB, ctx context.Context, logger echo.Logger) *Scheduler {
	return &Scheduler{
		ctx:              ctx,
		DB:               db,
		InvocationOutput: make(chan string, 100),
		tasks:            make(map[uint]*invocation),
		logger:           logger,
	}
}

//  Repair updates the next_run time of all the tasks in the db and returns the enabled tasks with their next run times
//  It should not be necessary to run this unless the next_run values are somehow corrupted.
func (s *Scheduler) Repair(now time.Time) ([]models.Task, error) {
	s.Lock()
	defer s.Unlock()
	s.logger.Info("Repairing next run times by computing them from previous invocations")
	var previousRuns []struct {
		ID       uint
		runStart *time.Time
	}
	err := s.DB.Raw("SELECT task_id, MAX(scheduled_at) FROM invocation GROUP BY task_id", &previousRuns).Error
	if err != nil {
		return nil, err
	}
	var runMap map[uint]*time.Time
	for _, run := range previousRuns {
		runMap[run.ID] = run.runStart
	}
	var tasks []models.Task
	err = s.DB.Where("enabled = 'true'").Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	for i := range tasks {
		var scheduleTime time.Time
		if runMap[tasks[i].ID] != nil {
			scheduleTime = runMap[tasks[i].ID].Add(time.Nanosecond)
		} else {
			scheduleTime = now
		}
		nextRun, err := tasks[i].NextInvocation(scheduleTime)
		if err != nil {
			s.logger.Warnf("Could not compute next invocation for invocation %s: %v", tasks[i].Name, err)
			continue
		}
		tasks[i].NextRun = &nextRun
		if err := tasks[i].Update(s.DB); err != nil {
			return nil, err
		}
		s.logger.Debugf("Task %s updated with new invocation time %v", tasks[i].Name, tasks[i].NextRun)
	}
	return tasks, nil
}

//  Next runs any tasks with a next_run before the given time. It returns the tasks it is running.
func (s *Scheduler) Next(now time.Time) ([]models.Task, error) {
	s.logger.Info("Starting scheduler loop")
	var tasks []models.Task
	err := s.DB.Where("enabled = ? AND next_run IS NOT NULL", true).Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	s.logger.Infof("There are %d enabled tasks", len(tasks))
	for _, task := range tasks {
		if task.NextRun.After(now) {
			s.logger.Infof("Task %s not scheduled to run yet", task.Name)
			continue
		}
		s.Lock()
		if task, ok := s.tasks[task.ID]; ok {
			if task.running == 1 {
				continue
			}
			task.running = 1
		}
		s.Unlock()
		s.beginInvocation(task, now)
	}
	return tasks, nil
}

func (s *Scheduler) beginInvocation(task models.Task, now time.Time){
	if task.NextRun == nil {
		s.logger.Warnf("Attempted to start invocation with nil Next Run time!")
		return
	}
	s.Lock()
	t, ok := s.tasks[task.ID]
	if !ok {
		t = &invocation{running:1}
		s.tasks[task.ID] = t
	}
	s.Unlock()
	//check that it hasn't been superceded by another invocation
	if ok && !t.lastExec.Before(*task.NextRun){
		s.logger.Debugf("Invocation for invocation %s time %v superceded by time %v", task.Name, task.NextRun, t.lastExec)
		return
	}
	t.lastExec = *task.NextRun
	var ctx context.Context
	//create new invocation
	ctx, t.cancel = context.WithCancel(s.ctx)

	s.logger.Infof("Starting invocation for invocation %s with run time %v", task.Name, task.NextRun)
	var i models.Invocation
	i.ScheduledAt = task.NextRun
	i.TaskID = task.ID
	i.Start = &now

	err := i.Create(s.DB)
	if err != nil {
		s.logger.Errorf("Could not create invocation in database: %v", err)
		return
	}
	if task.IsAQL {
		go s.runWithCtx(ctx, task, "analyst", "run", "--script", task.Command)
	} else {
		go s.runWithCtx(ctx, task, task.Command, task.Arguments)
	}
}

func (s *Scheduler) runWithCtx(ctx context.Context, t models.Task, name string, arg ...string) error {
	cmd := exec.CommandContext(ctx, name, arg...)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	cmd.Stdout = stdout
	err := cmd.Start()
	if err != nil {
		return s.endInvocation(t, time.Now(), err)
	}

	err = cmd.Wait()
	if err != nil {
		return s.endInvocation(t, time.Now(), err)
	}
	s.InvocationOutput <- stdout.String()
	s.InvocationOutput <- stderr.String()
	return s.endInvocation(t, time.Now(), nil)
}

func (s *Scheduler) endInvocation(t models.Task, now time.Time, withError error) error {
	var i models.Invocation
	err := s.DB.Where("task_id = ? AND scheduled_at = ?", t.ID, t.NextRun).First(&i).Error
	if err != nil {
		return err
	}
	i.Finish = &now
	if withError != nil {
		i.ErrorMessage = withError.Error()
	}
	var nextRun time.Time
	if t.Coalesce {
		nextRun, err = t.NextInvocation(now)
	} else {
		nextRun, err = t.NextInvocation(t.NextRun.Add(time.Nanosecond))
	}
	if err != nil {
		return err
	}
	t.NextRun = &nextRun
	err = t.Update(s.DB)
	if err != nil {
		return err
	}
	err = i.Update(s.DB)
	if err != nil {
		s.logger.Warnf("Could not write invocation to DB: %v", err)
	}
	if !t.Coalesce && t.NextRun.Before(now){
			//start next invocation immediately
			//this is recursive because beginInvocation calls endInvocation
			//s.logger.Debugf("Starting backlogged invocation for invocation %s with run time %v", t.Name, t.NextRun)
			s.beginInvocation(t, now)
	}
	s.Lock()
	tt := s.tasks[t.ID] //must exist
	tt.running = 0
	s.Unlock()
	return nil
}

// Cancel is a best effort to cancel the invocation. It will get restarted on the next Next() call.
func (s *Scheduler) Cancel(t models.Task) {
	s.Lock()
	defer s.Unlock()
	if t := s.tasks[t.ID]; t != nil && t.cancel != nil {
		t.cancel()
	}
	delete(s.tasks, t.ID)
}

func (s *Scheduler) Shutdown() {
	s.Lock()
	defer s.Unlock()
	for _, t := range s.tasks {
		if t != nil && t.cancel != nil {
			t.cancel()
		}
	}
	close(s.InvocationOutput)
}
