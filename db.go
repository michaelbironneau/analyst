package main

import (
    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/postgres"
)

//Group is a collection of users. A user may be belong to multiple groups.
type Group struct {
    gorm.Model 
    Name string
}

//User is a user of the system
type User struct {
    gorm.Model 
    Email string 
    Passhash string 
    IsAdmin bool
    IsAnalyst bool
    Groups []Group `gorm:"many2many:user_group"`
}

//Connection is a connection to an SQL database
type Connection struct {
    gorm.Model 
    Name string
    Description string
    Driver string
    ConnectionString string
}

//Template is a report template.
type Template struct {
    gorm.Model 
    Name string 
    Description string 
    Group Group 
    Script string `sql:"type:text"` 
}

//ReportProgress is the progress of a report that is currently being generated. When the report 
//has been generated Progress will be 100 and Message will be "Successfully generated" (or something
//to that effect).
type ReportProgress struct {
    ID int  
    Progress float64
    Message string
}

//Report is a generated report.
type Report struct {
    gorm.Model
    Filename string 
    Template Template 
    CreatedBy User
    Content string `sql:"type:text"` //base64 encoded content
    Status ReportProgress
}

//SetUpDatabase creates the necessary tables and indexes in the database
func SetUpDatabase(db *gorm.DB){
    db.AutoMigrate(&Group{}, &User{}, &Template{}, &ReportProgress{}, &Report{}, &Connection{})    
    db.Model(&Report{}).AddIndex("ix_report_created_at", "template_id", "created_at")
}

