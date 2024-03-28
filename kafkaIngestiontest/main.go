package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

func NewKafkaClient() *Kafka {
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireOne,
		MaxAttempts:            5,
		BatchSize:              1,
		Balancer:               kafka.Murmur2Balancer{},
		//BatchTimeout: time.Millisecond * 10,
	}
	return &Kafka{
		kafkaWriter: kafkaWriter,
	}
}

type Kafka struct {
	kafkaWriter *kafka.Writer
}

func (kf *Kafka) BatchPublish(messages []kafka.Message) error {
	fmt.Println(messages)
	return kf.kafkaWriter.WriteMessages(context.Background(), messages...)
}

func main() {
	kafkaClient := NewKafkaClient()
	kafkaTopic :=  "kafkaTopicName"
	TestFeatureInsert(kafkaClient, kafkaTopic, "schema/viewevents.avsc")
}

func TestFeatureInsert(kafkaClient *Kafka, kafkaTopic string, schemaPath string) {
	codec := getAvroCodecBySchemaPath(schemaPath)

	postIds := []string{"postid1", "postid2", "postid3", "postid4", "postid5"}

	for i, _ := range postIds {
		userid := "userid_1"
		if i%2 == 0 {
			userid = "userid_0"
		}
		d := fmt.Sprintf("counter _ %d", i)
		fmt.Println(d)
		row1 := map[string]interface{}{
			"userId":     goavro.Union("string", userid),
			"authorId":   goavro.Union("string", "authorId"),
			"postId":     goavro.Union("string", postIds[i]),
			"time":       goavro.Union("long", time.Now().UnixMilli()),
		}

		binary, err := codec.BinaryFromNative(nil, row1)
		if err != nil {
			fmt.Println("error while converting native data to avro binary", map[string]interface{}{
				"err":  err.Error(),
				"data": row1,
			})
		}
		err = kafkaClient.BatchPublish([]kafka.Message{
			{
				Topic: kafkaTopic,
				Value: binary,
			},
		})
		if err != nil {
			fmt.Println("error while publishing message to kafka", map[string]interface{}{
				"err":  err.Error(),
				"data": row1,
			})
		}
	}
}




func getAvroCodecBySchemaPath(filename string) *goavro.Codec {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("error in reading avro schema file", map[string]interface{}{
			"err": err.Error(),
		})
		return nil
	}
	avroSchema := string(data)
	avroCodec, err := goavro.NewCodecForStandardJSON(avroSchema)
	if err != nil {
		fmt.Println("error in loading the Avro schmea", map[string]interface{}{
			"err":      err.Error(),
			"fileName": filename,
		})
		return nil
	}
	return avroCodec
}
