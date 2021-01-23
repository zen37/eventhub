package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
//	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"gopkg.in/yaml.v2"
)

type yamlCfg struct {
	ConnEventHub string `yaml:"connEventHub"`
	File string `yaml:"logPartition"`
}

var (
	cfg     yamlCfg
	connStr string
	file string
)

func init() {
	cfg = yamlCfg{}

	yamlFile, err := ioutil.ReadFile("../.config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		log.Fatal(err)
	}
	
	connStr = cfg.ConnEventHub
	file = cfg.File
}

func main() {

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	hub, err := eventhub.NewHubFromConnectionString(connStr)
	if err != nil {
		log.Fatal(err)
	}

	//	getEventHubInfo(ctx, hub)

	exit := make(chan os.Signal, 1)

	err = listen(ctx, hub)
	if err != nil {
		fmt.Printf("failed to get runtime info: %s\n", err)
	}

	signal.Notify(exit, os.Interrupt, os.Kill)

	<-exit //waiting to receive something, anything

	fmt.Println(".....stopping.....")

	err = saveEventHubInfo(ctx, hub)
	if err != nil {
		log.Fatalln("saveEventHubInfo", err)
	}

	err = hub.Close(ctx)
	if err != nil {
		fmt.Println(err)
	}

}

func listen(ctx context.Context, hub *eventhub.Hub) error {

	// get info about the hub
	infoHub, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		return err
	}

	handler := func(c context.Context, event *eventhub.Event) error {
		fmt.Println(string(event.Data))
		return nil
	}

	for _, partitionID := range infoHub.PartitionIDs {
		// Start receiving messages
		fmt.Println("start receiving messages for partition", partitionID)
		// Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called
		// <- listenerHandle.Done() signals listener has stopped
		// listenerHandle.Err() provides the last error the receiver encountered

		//_, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
		_, err := hub.Receive(ctx, partitionID, handler)
		if err != nil {
			//fmt.Println(err)
			return err
		}
	}

	return nil

}

func getEventHubInfo(ctx context.Context, hub *eventhub.Hub) {

	hubManager, err := eventhub.NewHubManagerFromConnectionString(connStr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("Host: ", hubManager.Host)

	// get info about the hub
	infoHub, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		log.Fatalf("failed to get runtime info: %s\n", err)
	}

	fmt.Printf("\nPath: %v\nCreated at: %v\nPartition Count: %v\nPartition IDs: %v\n",
		infoHub.Path, infoHub.CreatedAt, infoHub.PartitionCount, infoHub.PartitionIDs)

	for _, p := range infoHub.PartitionIDs {
		// get info about the partitions
		infoPart, err := hub.GetPartitionInformation(ctx, p)
		if err != nil {
			log.Fatalf("failed to get partition info: %s\n", err)
		}
		fmt.Printf("PartitionID: %v\nBeginningSequenceNumber: %v\nLastSequenceNumber: %v\nLastEnqueuedOffset: %v\nLastEnqueuedTimeUtc: %v\n",
			infoPart.PartitionID, infoPart.BeginningSequenceNumber, infoPart.LastSequenceNumber, infoPart.LastEnqueuedOffset, infoPart.LastEnqueuedTimeUtc)
	}

}

func saveEventHubInfo(ctx context.Context, hub *eventhub.Hub) error {


	// get info about the hub
	infoHub, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		return err
		//log.Fatalf("failed to get runtime info: %s\n", err)
	}

	for _, p := range infoHub.PartitionIDs {
		// get info about the partitions
		infoPart, err := hub.GetPartitionInformation(ctx, p)
		if err != nil {
			return err
			//log.Fatalf("failed to get partition info: %s\n", err)
		}

		// If the file doesn't exist, create it, or append to the file
		f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		
		sep := "|"
		s := infoPart.PartitionID + sep + strconv.FormatInt(infoPart.BeginningSequenceNumber, 10) +
			sep + strconv.FormatInt(infoPart.LastSequenceNumber, 10) + sep + infoPart.LastEnqueuedOffset +
			sep + infoPart.LastEnqueuedTimeUtc.String()

		if _, err := f.WriteString(s + "\n"); err != nil {
			f.Close() // ignore error; Write error takes precedence
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
