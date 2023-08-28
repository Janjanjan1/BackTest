package main

import (
	"log"

	"github.com/pocketbase/pocketbase"
)

func main() {
	app := pocketbase.New()

	// app.OnBeforeServe().Add(func(e *core.ServeEvent) error {
	//     // add new "GET /hello" route to the app router (echo)
	//     e.Router.AddRoute(echo.Route{
	//         Method: http.MethodGet,
	//         Path:   "/hello",
	//         Handler: func(c echo.Context) error {
	//             return c.String(200, "Hello world!")
	//         },
	//         Middlewares: []echo.MiddlewareFunc{
	//             apis.ActivityLogger(app),
	//         },
	//     })

	//     return nil
	// })

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}
