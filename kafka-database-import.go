package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"os"
	"time"
	// Official 'mongo-go-driver' packages
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

)

var (

	DatabaseName        = ""
	DeviceDataCollectionName   = "deviceData"
	MongoHost           = "mongodb://127.0.0.1:27017"
	ContextTimeout = time.Duration(20)*time.Second
)
func NewDbContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), ContextTimeout)
	return ctx
}

func NewMongoStoreClient(mongoHost string) *mongo.Client {

	fmt.Println("Creating Mongo Store")
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoHost))
	if err != nil {
		fmt.Println("mongo.NewClient() ERROR:", err)
		os.Exit(1)
	}
	//ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
	ctx := NewDbContext()
	err = client.Connect(ctx)
	if err != nil {
		fmt.Println("mongo.Connect ERROR:", err)
		os.Exit(1)
	}
	fmt.Println("Created Mongo Store Successfully")

	return client

}

// XXX We should use go.common
func GetConnectionString() (string, error) {
	scheme, _ := os.LookupEnv("TIDEPOOL_STORE_SCHEME")
	hosts, _ := os.LookupEnv("TIDEPOOL_STORE_ADDRESSES")
	user, _ := os.LookupEnv("TIDEPOOL_STORE_USERNAME")
	password, _ := os.LookupEnv("TIDEPOOL_STORE_PASSWORD")
	optParams, _ := os.LookupEnv("TIDEPOOL_STORE_OPT_PARAMS")
	ssl, _ := os.LookupEnv("TIDEPOOL_STORE_TLS")


	var cs string
	if scheme != "" {
		cs = scheme + "://"
	} else {
		cs = "mongodb://"
	}

	if user != "" {
		cs += user
		if password != "" {
			cs += ":"
			cs += password
		}
		cs += "@"
	}

	if hosts != "" {
		cs += hosts
		cs += "/"
	} else {
		cs += "localhost/"
	}

	if ssl == "true" {
		cs += "?ssl=true"
	} else {
		cs += "?ssl=false"
	}

	if optParams != "" {
		cs += "&"
		cs += optParams
	}
	return cs, nil
}

type UserFilter struct {
	UserId       string          `bson:"userId" json:"userId"`
	Partition    int64           `bson:"partition" json:"partition"`
}

func importDatabase() {
	topic, _ := os.LookupEnv("KAFKA_TOPIC")
	partition := 0
	hostStr, _ := os.LookupEnv("KAFKA_BROKERS")
	dbName := "data"
	collectionName := "deviceData"

	userDbName := "user"
	userCollectionName := "userFilter"
	MaxRecs := 250000


	// Wait for networking
	fmt.Println("Waiting for networking")
	time.Sleep(10 * time.Second)

	startTime := time.Now()
	// Open Kafka
	fmt.Println("Opening connection to kafka host: ", hostStr, topic, partition)
	conn, err := kafka.DialLeader(context.Background(), "tcp", hostStr, topic, partition)

	fmt.Println("Done Opening connection to kafka")
	if err != nil {
		fmt.Printf("Error making connection: %s", err.Error())
		return
	}

	// Write out html
	fmt.Println("Setting write deadline")
	conn.SetWriteDeadline(time.Now().Add(4000*time.Second))

	// Open Database
	mongoHost, err := GetConnectionString()
	if err != nil {
		fmt.Print("Cound not connect to database: ", err)
		return
	}

	fmt.Println("Getting DB Client")
	client := NewMongoStoreClient(mongoHost)
	fmt.Println("Done getting DB Client")


	// Query to find user filter
	var userFilter []UserFilter
	fmt.Println("Retrieving user filter")
	userCollection := client.Database(userDbName).Collection(userCollectionName)
	userCur, err := userCollection.Find(context.Background(), bson.D{})
	defer userCur.Close(context.Background())
	if err != nil {
		fmt.Print("Retrieving User Filter Query failed: ", err)
		log.Fatal(err)
	}
	if err := userCur.All(context.Background(), &userFilter); err != nil {
		fmt.Print("Retrieving User Filter Query failed: ", err)
		log.Fatal(err)
	}

	var userIdArray []string
	for _, rec := range userFilter {
		userIdArray = append(userIdArray, rec.UserId)
	}
	fmt.Printf("User Filter Array: %v\n", userFilter)
	fmt.Println("UserIdArray: ", userIdArray)

	// Query to find records
	fmt.Println("Query records ")
	collection := client.Database(dbName).Collection(collectionName)
	cur, err := collection.Find(context.Background(), bson.M{"_userId": bson.M{"$in" : userIdArray}})
	if err != nil {
		fmt.Print("Getting Records failed Query failed: ", err)
		log.Fatal(err)
	}
	defer cur.Close(context.Background())
	fmt.Print("Query finished ")

	// loop through database
	i := 0
	for cur.Next(context.Background()) {
		// Only do first couple of records
		if i > MaxRecs {
			break
		}
		i++

		// Read record
		if err := cur.Err(); err != nil {
			fmt.Print("Error reading record: ", err)
			continue
		}
		var data bson.M
		err = cur.Decode(&data)
		if err != nil {
			log.Println("Error: ", err)
			continue
		}

		var rec = map[string]interface{}{"data": data, "source": "database"}
		document, err := json.Marshal(rec)
		if err != nil {
			log.Println("Error Marshalling: ", err)
			continue
		}


		// write to queue
		bytesWritten, err := conn.WriteMessages(
			kafka.Message{Value: []byte(document)},
		)
		if err != nil {
			fmt.Printf("Error processing %d record: \n", i)
			log.Println("Error writing message: ", bytesWritten, "  error: ", err)
		}
		if i % 1000 == 0{
			fmt.Printf("Processing %d record: \n", i)
			fmt.Printf("index: %d, doc: %s\n", i, document)
		}
	}
	fmt.Print("Done reading records")

	conn.Close()

	fmt.Printf("Duration in seconds: %f\n", time.Now().Sub(startTime).Seconds())

}

func main() {
	importDatabase()
	// Hack - do not quit for now
	fmt.Println("Sleeping until the end of time")
	for {
		time.Sleep(10 * time.Second)
	}
}