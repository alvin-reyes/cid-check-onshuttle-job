package main

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"net/http"
	"os"
)

var (
	// ErrInvalidCid is returned when a cid is invalid
	DB          *gorm.DB
	HostToCheck string
	// OsSignal signal used to shutdown
	OsSignal chan os.Signal
)

//	standalone library for properly aggregating CIDs and making storage deals for them on filc
func main() {

	//	 pings all the CIDs and logs the results
	shuttle := flag.String("shuttle", "shuttle-5.estuary.tech", "shuttle")
	numOfWorkers := flag.Int("numOfWorkers", 10, "numOfWorkers")
	fromDateRange := flag.String("fromDateRange", "2000-01-01", "fromDateRange")
	toDateRange := flag.String("toDateRange", "2023-12-31", "toDateRange")

	HostToCheck = *shuttle

	// 	initialize database
	var errOnDb error
	DB, errOnDb = setupDB()
	if errOnDb != nil {
		panic(errOnDb)
	}

	// 	query database for all the CIDs.
	cids, errOnQuery := QueryAllCidsFromContentsWithoutDealTable(*shuttle, *fromDateRange, *toDateRange)
	fmt.Println("Total CIDs on ", *shuttle, ": ", len(cids))
	if errOnQuery != nil {
		panic(errOnQuery)
	}

	var numJobs = len(cids)
	var perJob = numJobs / *numOfWorkers
	fmt.Println("numJobs", numJobs, "perJob", perJob)
	jobs := make(chan int, numJobs)
	results := make(chan int, numJobs)

	//	 slice the cids into groups
	var startCtr = 0
	var endCtr = perJob

	//	build the workers and process each slice of cids
	for w := 1; w <= *numOfWorkers; w++ {
		var rangeOfCids = (cids)[startCtr:endCtr]
		go worker(w, rangeOfCids, jobs, results)
		startCtr = endCtr + 1
		endCtr = endCtr + perJob
	}

	//	return the jobs
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	close(jobs)

	//	return the results
	for a := 1; a <= numJobs; a++ {
		<-results
	}
}

type Cids []struct {
	Cid DbCID
}

// > Query all the cids from the contents table without the deal table
func QueryAllCidsFromContentsWithoutDealTable(shuttle string, fromDate string, toDate string) (Cids, error) {
	var cids Cids

	err := DB.Raw("select c.cid as cid from contents as c where c.location = (select handle from shuttles as s where s.host = ?) and created_at between ? and ?", shuttle, fromDate, toDate).Scan(&cids).Error
	if err != nil {
		panic(err)
	}
	return cids, nil

}

// > This function sets up a database connection and returns a pointer to a gorm.DB object
func setupDB() (*gorm.DB, error) { // it's a pointer to a gorm.DB

	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()

	dbHost, okHost := viper.Get("DB_HOST").(string)
	dbUser, okUser := viper.Get("DB_USER").(string)
	dbPass, okPass := viper.Get("DB_PASS").(string)
	dbName, okName := viper.Get("DB_NAME").(string)
	dbPort, okPort := viper.Get("DB_PORT").(string)
	if !okHost || !okUser || !okPass || !okName || !okPort {
		panic("invalid database configuration")
	}

	dsn := "host=" + dbHost + " user=" + dbUser + " password=" + dbPass + " dbname=" + dbName + " port=" + dbPort + " sslmode=disable TimeZone=Asia/Shanghai"

	DB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return DB, nil
}

// 	Simple (but efficient) worker?
func worker(id int, groupCid Cids, jobs <-chan int, results chan<- int) {
	for j := range jobs {
		// IPLD get
		for _, cid := range groupCid {
			fmt.Println("worker", id, "started  job", j, "cid", cid)
			//node, err := peer.Get(context.Background(), cid.Cid.CID) // pull!!
			resp, err := http.Get("https://" + HostToCheck + "/gw/ipfs/" + cid.Cid.CID.String())
			if err != nil {
				fmt.Println("worker:", id, ",failed  job:", j, ", cid:", cid, ",resp:", err.Error())
				continue
			}
			if resp.StatusCode == 200 {
				fmt.Println("worker:", id, ",completed  job:", j, ", cid:", cid, ",resp:", resp.StatusCode)
			}
			if resp.StatusCode != 200 {
				fmt.Println("worker:", id, ",failed  job:", j, ", cid:", cid, ",resp:", resp.StatusCode)
			}
		}

		//	log the result somewhere? maybe a SQLite db.
		fmt.Println("worker", id, "finished job", j)
		results <- j * 2
	}
}
