package main

import (
	"context"
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

func importDatabase() {
	topic := "data"
	partition := 1
	host := "kafka-kafka-bootstrap.kafka.svc.cluster.local"
	port := 9092
	hostStr := fmt.Sprintf("%s:%d", host,port)
	dbName := "data"
	collectionName := "deviceData"

	// Open Database
	mongoHost, err := GetConnectionString()
	if err != nil {
		fmt.Print("Cound not connect to database: ", err)
		return
	}

	fmt.Println("Getting DB Client")
	client := NewMongoStoreClient(mongoHost)
	fmt.Println("Done getting DB Client")

	// Open Kafka
	fmt.Println("Opening connection to kafka")
	conn, err := kafka.DialLeader(context.Background(), "tcp", hostStr, topic, partition)
	fmt.Println("Done Opening connection to kafka")
	if err != nil {
		fmt.Printf("Error making connection: %s", err.Error())
		return
	}

	// Write out html
	fmt.Println("Setting write deadline")
	conn.SetWriteDeadline(time.Now().Add(10*time.Second))

	// Query to find records
	fmt.Println("Query records ")
	collection := client.Database(dbName).Collection(collectionName)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		fmt.Print("Query failed: ", err)
		log.Fatal(err)
	}
	defer cur.Close(context.Background())
	fmt.Print("Query finished ")

	// loop through database
	i := 0
	for cur.Next(context.Background()) {
		fmt.Printf("Processing %d record: ", i)
		// Only do first couple of records
		if i > 4 {
			break
		}
		i++

		// Read record
		raw := cur.Current

		if err := cur.Err(); err != nil {
			fmt.Print("Error reading record: ", err)
			break
		}
		// write to queue
		conn.WriteMessages(
			kafka.Message{Value: raw},
		)
		fmt.Printf("doc: %s\n", raw)
	}
	fmt.Print("Done reading records")

	conn.Close()

}

func main() {
	importDatabase()
	// Hack - do not quit for now
	fmt.Println("Sleeping until the end of time")
	for {
		time.Sleep(10 * time.Second)
	}
}