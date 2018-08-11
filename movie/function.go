package movie

import (
	"fmt"
	"log"
)

func (i *Instance) generateKey(movieID int) (result string) {
	return fmt.Sprintf(i.keyPattern, movieID)
}

func (i *Instance) GetMovieInfo(movieID int) (result Movie) {
	result = Movie{}

	tmpResult, err := i.kCache.Get(i.generateKey(movieID))
	if err != nil {
		log.Println("[error]", err, movieID)
		return
	}

	if v, ok := tmpResult.(Movie); ok {
		result = v
	} else {
		log.Println("[error] failed to parse into Movie", tmpResult)
	}

	return
}
