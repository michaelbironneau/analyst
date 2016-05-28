package main

import (
	"encoding/base64"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/labstack/echo"
	"net/http"
	"strconv"
	"time"
)

//Group is a collection of users. A user may be belong to multiple groups.
type Group struct {
	gorm.Model
	Name string `gorm:"unique"`
}

func (g Group) Delete(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	groupID := c.Param("group_id")
	
	if !currentUser.IsAdmin {
		return fmt.Errorf("Only administrative users may delete groups")
	}
	
	iid, err := strconv.Atoi(groupID)
	
	if err != nil {
		return fmt.Errorf("Invalid group id")
	}
	var gp Group 
	g.ID = uint(iid)
	return db.Unscoped().Delete(&gp).Error
}

//List is a DataFunc to list all groups
func (g Group) List(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	var groups []Group
	if err := db.Find(&groups).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Groups": groups}, nil
}

func (g Group) Save(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	user, _ := c.Get("user").(User)
	groupName := c.FormValue("name")
	groupID := c.FormValue("group_id")
	
	if len(groupID) == 0 {
		groupID = c.Param("group_id") //it is valid to pass the ID through form value or param
	}
	
	if !user.IsAdmin {
		return fmt.Errorf("Only administrative users can modify groups")
	}
	if len(groupName) == 0 {
		return fmt.Errorf("Group name cannot be empty")
	}
	var group Group	
	var iid int
	var err error
	group.Name = groupName
	
	if len(groupID) > 0 {
		iid, err = strconv.Atoi(groupID)
		if err != nil || iid < 0 {
			return fmt.Errorf("Invalid group ID")
		}
	group.ID = uint(iid)	
	}
	
	if err = db.Save(group).Error; err != nil {
		return fmt.Errorf("Error saving group")
	}
	return nil
}

//Get is a DataFunc to retrieve a single group
func (g Group) Get(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	id := c.Param("group_id")
	iid, err := strconv.Atoi(id)
	if err != nil {
		return nil, fmt.Errorf("Invalid user id")
	}
	var group Group
	if err := db.First(&group, uint(iid)).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Group": group}, nil
}


//User is a user of the system
type User struct {
	gorm.Model
	Login     string `gorm:"unique"`
	Passhash  string `gorm:"column:passhash"`
	IsAdmin   bool
	IsAnalyst bool
	Groups    []Group `gorm:"many2many:user_group"`
}

func (g User) Delete(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	userID := c.Param("user_id")
	
	if !currentUser.IsAdmin {
		return fmt.Errorf("Only administrative users may delete users")
	}
	
	
	iid, err := strconv.Atoi(userID)
	
	if err != nil {
		return fmt.Errorf("Invalid user id")
	}
	
	if uint(iid) == currentUser.ID {
		return fmt.Errorf("A user may not delete themselves")
	}
	var user User
	user.ID = uint(iid)
	return db.Unscoped().Delete(&user).Error
}

func (g User) Save(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	userID := c.FormValue("user_id")
	
	if len(userID) == 0 {
		userID = c.Param("user_id") //it is valid to pass the ID through form value or param
	}
	
	if !currentUser.IsAdmin && (userID != strconv.Itoa(int(currentUser.ID))) {
		return fmt.Errorf("Only administrative users can modify other users")
	}
	
	var (
		login string
		password string 
		isAdmin string 
		isAnalyst string
		user User
	)
	login = c.FormValue("login")
	password = c.FormValue("password")
	isAdmin = c.FormValue("is_admin")
	isAnalyst = c.FormValue("is_analyst")
	
	if len(userID) == 0 {
		//creation 
		if len(login) == 0 {
			return fmt.Errorf("Login cannot be empty")
		}
		user.Login = login 
		if len(password) < 8 {
			return fmt.Errorf("Password must have at least 8 characters")
		}
		h, err := hashPassword(password)
		if err != nil {
				return err
			}
		user.Passhash = h
		var bErr error
		user.IsAdmin, bErr = strconv.ParseBool(isAdmin)
		if bErr != nil {
			return fmt.Errorf("Invalid value for is_admin")
		} 
		user.IsAnalyst, bErr = strconv.ParseBool(isAnalyst)
		if bErr != nil {
			return fmt.Errorf("Invalid valud for is_analyst")
		}
		
		return db.Create(&user).Error 
		
	}
	
	if len(userID) > 0 {
		iid, err := strconv.Atoi(userID)
		if err != nil || iid < 0 {
			return fmt.Errorf("Invalid user ID")
		}
		user.ID = uint(iid)	
	}
	tx := db.Begin()
	var txFailure bool
	if len(userID) > 0 {
		//update 
		if len(login) > 0 {
			txFailure = txFailure || (tx.Model(&user).Update("login", login).Error != nil)
		}
		if len(password) > 0 && len(password) >= 8 {
			h, err := hashPassword(password)
			if err != nil {
				tx.Rollback()
				return err
			}
			tx.Model(&user).Update("passhash", h)
		} else if len(password) < 8 {
			tx.Rollback()
			return fmt.Errorf("Password must be at least 8 characters")
		}
		if len(isAdmin) > 0 {
			v, err := strconv.ParseBool(isAdmin)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("Invalid value for is_admin")
			}
			txFailure = txFailure || (tx.Model(&user).Update("is_admin", v).Error != nil)
		}
		if len(isAnalyst) > 0 {
			v, err := strconv.ParseBool(isAnalyst)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("Invalid value for is_analyst")
			}
			txFailure = txFailure || (tx.Model(&user).Update("is_analyst", v).Error != nil) 
		}
		if txFailure {
			tx.Rollback()
			return fmt.Errorf("Unexpected error saving user")
		} else {
			tx.Commit()
			return nil
		}
		
	}
	return nil
}

//List is a DataFunc to list all groups
func (g User) List(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	var users []User
	if err := db.Find(&users).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Users": users}, nil
}

//Get is a DataFunc to retrieve a single group
func (g User) Get(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	id := c.Param("user_id")
	iid, err := strconv.Atoi(id)
	if err != nil {
		return nil, fmt.Errorf("Invalid user id")
	}
	u, _ := getCurrentUser(c)
	if !(u.IsAdmin || u.ID == uint(iid)) {
		return nil, fmt.Errorf("Unauthorized")
	}
	var user User
	if err := db.First(&user, uint(iid)).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"User": user}, nil
}

//Connection is a connection to an SQL database
type Connection struct {
	gorm.Model
	Name             string `gorm:"unique"`
	Description      string
	Driver           string
	ConnectionString string
}

func (g Connection) Delete(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	connectionID := c.Param("connection_id")
	
	if !(currentUser.IsAdmin || currentUser.IsAnalyst) {
		return fmt.Errorf("Only administrative users and analysts may delete connections")
	}
	
	
	iid, err := strconv.Atoi(connectionID)
	
	if err != nil {
		return fmt.Errorf("Invalid connection id")
	}
	
	var connection Connection
	connection.ID = uint(iid)
	return db.Unscoped().Delete(&connection).Error
}


//List is a DataFunc to list all connection
func (g Connection) List(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	var connections []Connection
	if err := db.Find(&connections).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Connections": connections}, nil
}

//Get is a DataFunc to retrieve a single connection
func (g Connection) Get(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	id := c.Param("connection_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	var connection Connection
	if err := db.First(&connection, uid).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Connection": connection}, nil
}

//Template is a report template.
type Template struct {
	gorm.Model
	Name string `gorm:"unique"`
	Content string `sql:"type:text"`
}

func (g Template) Delete(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	templateID := c.Param("template_id")
	
	if !(currentUser.IsAdmin || currentUser.IsAnalyst) {
		return fmt.Errorf("Only administrative users and analysts may delete templates")
	}
	
	
	iid, err := strconv.Atoi(templateID)
	
	if err != nil {
		return fmt.Errorf("Invalid template id")
	}
	
	var template Template
	template.ID = uint(iid)
	return db.Unscoped().Delete(&template).Error
}


//Script is a report script.
type Script struct {
	gorm.Model
	Name        string `gorm:"unique"`
	Description string
	Group       Group
	Content     string `sql:"type:text"`
}


func (g Script) Delete(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	scriptID := c.Param("script_id")
	
	if !(currentUser.IsAdmin || currentUser.IsAnalyst) {
		return fmt.Errorf("Only administrative users and analysts may delete scripts")
	}
	
	
	iid, err := strconv.Atoi(scriptID)
	
	if err != nil {
		return fmt.Errorf("Invalid script id")
	}
	
	var script Script
	script.ID = uint(iid)
	return db.Unscoped().Delete(&script).Error
}

//List is a DataFunc to list all scripts
func (g Template) List(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	var templates []Template
	if err := db.Find(&templates).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Templates": templates}, nil
}

//Get is a DataFunc to retrieve a single script
func (g Template) Get(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	id := c.Param("template_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	var template Template
	if err := db.First(&template, uid).Error; err != nil {
		return nil, err
	}
	return map[string]interface{}{"Template": template}, nil
}

//List is a DataFunc to list all templates
func (g Script) List(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	user, _ := getCurrentUser(c)
	var scripts []Script
	var groups []Group
	if err := db.Model(&user).Related(&groups).Error; err != nil {
		return nil, err
	}

	for i := range groups {
		var script Script
		if !db.Model(&groups[i]).Related(&script).RecordNotFound() {
			scripts = append(scripts, script)
		}
	}

	return map[string]interface{}{"Scripts": scripts}, nil
}

//Get is a DataFunc to retrieve a single connection
func (g Script) Get(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	user, _ := getCurrentUser(c)
	id := c.Param("script_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	var script Script
	if err := db.First(&script, uint(uid)).Error; err != nil {
		return nil, err
	}
	var found bool
	for i := range user.Groups {
		if user.Groups[i].ID == script.ID {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("User not authorized to view script")
	}
	return map[string]interface{}{"Script": script}, nil
}

func (g Script) Download(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	user, _ := getCurrentUser(c)
	if !(user.IsAnalyst || user.IsAdmin) {
		return fmt.Errorf("Only analysts and administrators are authorized to download scripts")
	}
	id := c.Param("script_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return err
	}
	var content string
	err = db.First(&Script{}, uid).Pluck("content", &content).Error
	if err != nil {
		return err
	}
	var ret []byte
	if len(content) == 0 {
		return c.Render(http.StatusNoContent, "error", map[string]interface{}{"Message": "Report content is not yet available"})
	}
	ret, err = base64.StdEncoding.DecodeString(content)

	if err != nil {
		return err
	}

	//MIME type here: https://blogs.msdn.microsoft.com/vsofficedeveloper/2008/05/08/office-2007-file-format-mime-types-for-http-content-streaming-2/
	c.Response().Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	c.Response().Header().Set("Content-Disposition", "attachment; filename=\"script\"")
	c.Response().Write(ret)
	return nil
}


//ReportProgress is the progress of a report that is currently being generated. When the report
//has been generated Progress will be 100 and Message will be "Successfully generated" (or something
//to that effect).
type ReportProgress struct {
	ID       int
	Progress float64
	Message  string
}

//Report is a generated report.
type Report struct {
	gorm.Model
	Filename  string
	Template  Template
	CreatedBy User
	Content   string `sql:"type:text"` //base64 encoded content
	Status    ReportProgress
}

type ReportListItem struct {
	ReportID  uint
	Filename  string
	CreatedBy string
	CreatedAt time.Time
}

func (g Report) Delete(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	currentUser, _ := c.Get("user").(User)
	reportID := c.Param("report_id")
	
	if !(currentUser.IsAdmin || currentUser.IsAnalyst) {
		return fmt.Errorf("Only administrative users and analysts may delete reports")
	}
	
	
	iid, err := strconv.Atoi(reportID)
	
	if err != nil {
		return fmt.Errorf("Invalid report id")
	}
	
	var report Report
	report.ID = uint(iid)
	return db.Unscoped().Delete(&report).Error
}

//List is a DataFunc to list all reports for a given template
func (g Report) List(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	user, _ := getCurrentUser(c)
	id := c.Param("template_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	rows, err := db.Raw(`
		SELECT r.id, r.filename, u.login, r.created_at FROM dbo.users l
			LEFT JOIN dbo.user_group g ON g.user_id = l.id  
			LEFT JOIN dbo.templates t ON t.group_id = g.group_id 
			LEFT JOIN dbo.reports r ON r.template_id = t.id 
			LEFT JOIN dbo.users u ON u.id = r.created_by
		WHERE l.id = ? r.id = ?
	`, user.ID, uint(uid)).Rows()
	if err != nil {
		return nil, err
	}

	var ret []ReportListItem
	defer rows.Close()
	for rows.Next() {
		var rli ReportListItem
		rows.Scan(&rli.ReportID, &rli.Filename, &rli.CreatedBy, &rli.CreatedAt)
		ret = append(ret, rli)
	}

	return map[string]interface{}{"Reports": ret}, nil
}

//Get is a DataFunc to get the report
func (g Report) Get(c echo.Context) (map[string]interface{}, error) {
	db := c.Get("db").(gorm.DB)
	user, _ := getCurrentUser(c)
	id := c.Param("report_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	rows, err := db.Raw(`
		SELECT r.id, r.filename, u.login, r.created_at FROM dbo.users l
			LEFT JOIN dbo.user_group g ON g.user_id = l.id  
			LEFT JOIN dbo.templates t ON t.group_id = g.group_id 
			LEFT JOIN dbo.reports r ON r.template_id = t.id 
			LEFT JOIN dbo.users u ON u.id = r.created_by
		WHERE l.id = ? r.id = ?
	`, user.ID, uint(uid)).Rows()
	if err != nil {
		return nil, err
	}

	var ret []ReportListItem
	defer rows.Close()
	for rows.Next() {
		var rli ReportListItem
		rows.Scan(&rli.ReportID, &rli.Filename, &rli.CreatedBy, &rli.CreatedAt)
		ret = append(ret, rli)
	}

	return map[string]interface{}{"Reports": ret}, nil
}

func (g Report) Download(c echo.Context) error {
	db := c.Get("db").(gorm.DB)
	user, _ := getCurrentUser(c)
	id := c.Param("report_id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		return err
	}
	var content string
	var filename string
	row := db.Raw(`
		SELECT TOP 1 r.filename, r.content FROM dbo.users l
			LEFT JOIN dbo.user_group g ON g.user_id = l.id  
			LEFT JOIN dbo.templates t ON t.group_id = g.group_id 
			LEFT JOIN dbo.reports r ON r.template_id = t.id 
			LEFT JOIN dbo.users u ON u.id = r.created_by
		WHERE l.id = ? AND r.id = ?	
	`, user.ID, uint(uid)).Row()
	if err != nil {
		return err
	}
	row.Scan(&filename, &content)
	var ret []byte
	if len(content) == 0 {
		return c.Render(http.StatusNoContent, "error", map[string]interface{}{"Message": "Report content is not yet available"})
	}
	ret, err = base64.StdEncoding.DecodeString(content)

	if err != nil {
		return err
	}

	//MIME type here: https://blogs.msdn.microsoft.com/vsofficedeveloper/2008/05/08/office-2007-file-format-mime-types-for-http-content-streaming-2/
	c.Response().Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	
	c.Response().Header().Set("Content-Disposition", "attachment; filename=\"" + filename + "\"")
	c.Response().Write(ret)
	return nil
}

//autoMigrateDatabase creates the necessary tables and indexes in the database
func autoMigrateDatabase(db *gorm.DB) {
	db.AutoMigrate(&Group{}, &User{}, &Template{}, &ReportProgress{}, &Report{}, &Connection{})
	db.Model(&Report{}).AddIndex("ix_report_created_at", "template_id", "created_at")
}
