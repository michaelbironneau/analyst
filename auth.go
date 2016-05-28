package main

import (
	"fmt"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"time"
)

//validateLogin validates a login attempt
func validateLogin(next echo.HandlerFunc) echo.HandlerFunc {

	return func(c echo.Context) error {
		login := c.FormValue("user")
		password := c.FormValue("password")

		if len(login) == 0 || len(password) == 0 {
			return next(c)
		}

		db := c.Get("db").(*gorm.DB)
		var user User

		if err := db.Where(&User{Login: login}).First(&user).Error; err != nil {
			return c.Render(403, "unauthorized", nil)
		}

		if err := bcrypt.CompareHashAndPassword([]byte(user.Passhash), []byte(password)); err != nil {
			return c.Render(403, "unauthorized", nil)
		}

		c.Set("user", user)
		return next(c)
	}
}

//getCurrentUser retrieves the current user
func getCurrentUser(c echo.Context) (*User, error) {

	//first try cached value in context
	var user User
	user, ok := c.Get("user").(User)
	if ok {
		return &user, nil
	}

	//retrieve from database
	db := c.Get("db").(*gorm.DB)
	login := c.Get("login").(string)
	if err := db.Where(&User{Login: login}).First(&user).Error; err != nil {
		return nil, fmt.Errorf("Error retrieving user records")
	}
	//cache
	c.Set("user.admin", user.IsAdmin)
	c.Set("user.analyst", user.IsAnalyst)

	return &user, nil
}

//setCookie sets the cookie after the user has been authenticated
func setCookie(c echo.Context) error {
	token := jwt.New(jwt.SigningMethodHS256)
	user := c.Get("user").(User)
	expiry := time.Now().Add(time.Hour * 24)
	token.Claims["user"] = user.Login
	token.Claims["exp"] = expiry.Unix()
	tokenString, err := token.SignedString(Config.SigningKey)
	cookie := new(echo.Cookie)
	cookie.SetExpires(expiry)
	cookie.SetName("analyst")
	cookie.SetValue(tokenString)
	c.SetCookie(cookie)
	return err
}

//authenticate gets the authenticated user and stores it in context data.
func authenticate(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		cookie, err := c.Cookie("analyst")

		if err != nil {
			return c.Render(http.StatusForbidden, "login", nil)
		}

		if cookie.Expires().Before(time.Now()) {
			return c.Render(http.StatusForbidden, "login", nil)
		}

		v := cookie.Value()

		t, err := jwt.Parse(v, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("Invalid signnig method")
			}
			return token.Header["user"], nil
		})

		if !t.Valid || err != nil {
			return c.Render(http.StatusForbidden, "login", nil)
		}
		var (
			login string
			ok    bool
		)
		if login, ok = t.Header["user"].(string); !ok {
			return c.Render(http.StatusForbidden, "login", nil)
		}

		c.Set("login", login)

		return next(c)
	}
}
