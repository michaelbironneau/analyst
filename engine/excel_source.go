package engine

import (
	"errors"
	xlsx "github.com/360EntSecGroup-Skylar/excelize"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	ErrExcelTooManyWildcards     = errors.New("the Excel source/destination range can have at most one wildcard")
	ErrExcelCannotIncludeColumns = errors.New("the Excel source range cannot be dynamic in X if it includes columns")
	ErrColumnsNotSpecified       = errors.New("the Excel range should either include columns or they should be specified in the COLUMNS option")
	fileManager                  *excelFileManager
)

const DefaultExcelDateFormat = time.RFC3339

func init() {
	fileManager = newExcelFileManager()
}

type excelFile struct {
	sync.Mutex
	F *xlsx.File
}

//ExcelFileManager is a singleton that manages pointers to Excel files.
//The reason for this is that if we have multiple goroutines writing to the same
//file (a common use case when building an Excel spreadsheet), they will all
//be clashing as only one can hold a lock on the file until all the others are done.
// This way, we have multiple goroutines able to make progress by incrementally
// writing the file.
type excelFileManager struct {
	sync.RWMutex
	files map[string]*excelFile
}

func newExcelFileManager() *excelFileManager {
	return &excelFileManager{
		files: make(map[string]*excelFile),
	}
}

//Register registers the file with the manager. Idempotent.
func (e *excelFileManager) Register(filename string, create bool) error {
	e.Lock()
	defer e.Unlock()
	if _, ok := e.files[filename]; ok {
		return nil
	}
	var (
		f *xlsx.File
		err error
	)
	if create {
		f = xlsx.NewFile()
	} else {
		f, err = xlsx.OpenFile(filename)
	}
	if err != nil {
		return err
	}
	e.files[filename] = &excelFile{
		F: f,
	}
	return nil
}

//Use applies the given func to the excel file, holding a lock while it does.
func (e *excelFileManager) Use(filename string, f func(*xlsx.File)) {
	e.RLock()
	ff := e.files[filename]
	if f == nil {
		panic("didn't register Excel file before using it!")
	}
	e.RUnlock()
	ff.Lock()
	defer ff.Unlock()
	f(ff.F)
}

type ExcelRangePoint struct {
	N bool //wildcard
	P int
}

type ExcelRange struct {
	X1 int
	Y1 int
	X2 ExcelRangePoint
	Y2 ExcelRangePoint
}

type ExcelSource struct {
	Name                 string
	Filename             string
	Sheet                string
	Range                ExcelRange
	RangeIncludesColumns bool
	Dateformat           string
	Cols                 []string
	posX                 int
	posY                 int
	outgoingName         string
}

func (s *ExcelSource) SetName(name string){
	s.outgoingName = name
}

func (s *ExcelSource) Ping() error {
	if s.Range.X2.N && s.Range.Y2.N {
		return ErrExcelTooManyWildcards
	}
	if s.RangeIncludesColumns && s.Range.X2.N {
		return ErrExcelCannotIncludeColumns
	}
	if _, err := os.Stat(s.Filename); err != nil {
		return err
	}
	return nil
}

func (s *ExcelSource) fatalerr(err error, st Stream, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  s.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(st.Chan(s.outgoingName))
}

func (s *ExcelSource) Open(dest Stream, logger Logger, stop Stopper) {
	err := fileManager.Register(s.Filename, false)
	if err != nil {
		s.fatalerr(err, dest, logger)
		return
	}
	logger.Chan() <- Event{
		Level:   Trace,
		Time:    time.Now(),
		Message: "Excel source opened",
	}
	s.posX = s.Range.X1
	s.posY = s.Range.Y1
	if s.RangeIncludesColumns {
		s.Cols = s.scanColumns()
		logger.Chan() <- Event{
			Level:   Trace,
			Time:    time.Now(),
			Message: "Columns scanned",
		}
		//set position to first cell in second row of range
		s.posY++
		s.posX = s.Range.X1
	} else if s.Cols == nil {
		s.fatalerr(ErrColumnsNotSpecified, dest, logger)
		return
	}

	dest.SetColumns(s.outgoingName, s.Cols)
	c := dest.Chan(s.outgoingName)
	for {
		if stop.Stopped() {
			break
		}
		var msg []interface{}
		var nonEmptyRow bool
		for x := s.Range.X1; x <= s.Range.X2.P; x++ {
			fileManager.Use(s.Filename, func(e *xlsx.File) {
				v, empty := s.convertCellValue(e.GetCellValue(s.Sheet, pointToCol(x, s.posY)))
				nonEmptyRow = nonEmptyRow || (!empty)
				msg = append(msg, v)
			})
		}
		if nonEmptyRow || !s.Range.Y2.N {
			c <- Message{Source: s.outgoingName, Data: msg}
		}

		if s.Range.Y2.N && nonEmptyRow {
			s.posY++
		} else if s.posY < s.Range.Y2.P {
			s.posY++
		} else {
			break //break on first out-of-range row or empty row (if range is dynamic)
		}
	}
	close(c)
}

//scanColumns scans the cell contents into a slice of string, assuming
//that we are in the
func (s *ExcelSource) scanColumns() []string {
	var cols []string
	fileManager.Use(s.Filename, func(e *xlsx.File) {
		for {
			col := e.GetCellValue(s.Sheet, pointToCol(s.posX, s.posY))
			if len(col) > 0 {
				cols = append(cols, col)
			} else {
				return
			}
			if s.posX < s.Range.X2.P {
				s.posX++
			} else {
				return
			}
		}
	})
	return cols
}

func (s *ExcelSource) convertCellValue(val string) (interface{}, bool) {
	var empty bool

	if val == "" {
		empty = true
	}

	//int
	i, err := strconv.Atoi(val)

	if err == nil {
		return i, empty
	}

	//float64
	f, err := strconv.ParseFloat(val, 64)

	if err == nil {
		return f, empty
	}

	//boolean
	b, err := strconv.ParseBool(val)

	if err == nil {
		return b, empty
	}

	//time.Time

	if len(s.Dateformat) > 0 {
		d, err := time.Parse(s.Dateformat, val)

		if err == nil {
			return d, empty
		}
	} else {
		d, err := time.Parse(DefaultExcelDateFormat, val)

		if err == nil {
			return d, empty
		}
	}

	//string
	return val, empty
}

func pointToCol(x, y int) string {
	return xlsx.ToAlphaString(x-1) + strconv.Itoa(y)
}

func (s *ExcelSource) Columns() []string {
	return s.Cols
}
