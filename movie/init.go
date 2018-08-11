package movie

import (
	"log"
	"strconv"
	"strings"

	"github.com/koding/cache"
)

func New() (i *Instance) {
	i = &Instance{
		kCache:     cache.NewMemory(),
		keyPattern: "movie:%d",
	}

	i.generateToCache()
	return
}

func (i *Instance) generateToCache() {
	movieDatasetsSplitted := strings.Split(movieDatasets, "\n")
	for _, mds := range movieDatasetsSplitted {
		movieAttr := strings.Split(mds, ",")
		if len(movieAttr) > 3 {
			tmpName := strings.Join(movieAttr[1:len(movieAttr)-1], ",")
			tmpName = strings.Replace(tmpName, `"`, "", -1)

			tmpMovieAttr := movieAttr
			movieAttr = []string{}
			movieAttr = append(movieAttr, tmpMovieAttr[0])
			movieAttr = append(movieAttr, tmpName)
			movieAttr = append(movieAttr, tmpMovieAttr[len(tmpMovieAttr)-1])
		} else if len(movieAttr) < 3 {
			log.Println("[error] invalid schema", mds)
			continue
		}

		movid, errParse := strconv.Atoi(movieAttr[0])
		if errParse != nil {
			log.Println("[error]", errParse, movieAttr)
			continue
		}
		genres := strings.Split(movieAttr[2], "|")
		movieInfo := Movie{
			ID:     movid,
			Name:   movieAttr[1],
			Genres: genres,
		}
		i.kCache.Set(i.generateKey(movid), movieInfo)
	}
}
