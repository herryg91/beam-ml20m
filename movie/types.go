package movie

import "github.com/koding/cache"

type Instance struct {
	kCache     cache.Cache
	keyPattern string
}
type Movie struct {
	ID     int
	Name   string
	Genres []string
}
