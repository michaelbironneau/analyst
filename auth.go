package main 

import (
	"github.com/labstack/echo"
	jwt "github.com/dgrijalva/jwt-go"
	"fmt"
	"time"
	"github.com/jinzhu/gorm"
	"golang.org/x/crypto/bcrypt"
)

//validateLogin validates a login attempt
func validateLogin(c echo.Context) error {
	login := c.FormValue("user")
	password := c.FormValue("password")
	
	if len(login) == 0 || len(password) == 0 {
		return c.Render(403, "unauthorized", nil)
	}
	
	db := c.Get("db").(*gorm.DB)
	var user User
	
	
	if err := db.Where(&User{Login:login}).First(&user).Error; err != nil {
		return fmt.Errorf("Error retrieving user records")
	}
	
	if err := bcrypt.CompareHashAndPassword([]byte(user.Passhash), []byte(password)); err != nil {
		return c.Render(403, "unauthorized", nil)	
	}
	
	c.Set("user", login)
	return nil
}

//setCookie sets the cookie after the user has been authenticated
func setCookie(c echo.Context) error {
	token := jwt.New(jwt.SigningMethodHS256)
	user := c.Get("user").(string)
	expiry :=  time.Now().Add(time.Hour*24)
	token.Claims["user"] = user 
	token.Claims["exp"] = expiry.Unix()
	tokenString, err := token.SignedString(Config.SigningKey)
	cookie := new(echo.Cookie)
	cookie.SetExpires(expiry)
	cookie.SetName("analyst")
	cookie.SetValue(tokenString)
	c.SetCookie(cookie)
	return err
}

//getUser gets the authenticated user and stores it in context data.
func getUser(c echo.Context) error {
	cookie, err := c.Cookie("analyst")
	
	if err != nil {
		return fmt.Errorf("Invalid user authentication (1)")
	}
	
	if cookie.Expires().Before(time.Now()){
		return fmt.Errorf("Invalid user authentication (2)")
	}
	
	v := cookie.Value()
	
	t, err := jwt.Parse(v, func(token *jwt.Token) (interface{}, error){
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Invalid user authentication (3)")
		}
		return token.Header["user"], nil
	})
	
	if !t.Valid || err != nil {
		return fmt.Errorf("Invalid user authentication (4)")
	}
	
	if user, ok := t.Header["user"].(string); !ok {
		return fmt.Errorf("Invalid user authentication (5)")
	} else {
		c.Set("user", user)
	}	
	return nil
}