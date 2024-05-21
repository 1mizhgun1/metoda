package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
)

const (
	KafkaAddr        = "localhost:29092"
	KafkaTopic       = "segments"
	KafkaReadPeriod  = 2 * time.Second
	SegmentSize      = 100
	SegmentLostError = "lost"
	CodeUrl          = "http://192.168.123.120:8000/code"
	ReceiveUrl       = "http://192.168.123.140:3000/receive" // адрес websocket-сервера прикладного уровн
)

type Segment struct {
	SegmentNumber  int       `json:"segment_number"`
	TotalSegments  int       `json:"total_segments"`
	Username       string    `json:"username"`
	SendTime       time.Time `json:"send_time"`
	SegmentPayload string    `json:"payload"`
}

type Message struct {
	Received int
	Total    int
	Last     time.Time
	Username string
	Segments []string
}

// ReceiveRequest - структура тела запроса на прикладной уровень
type ReceiveRequest struct {
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
	Error    string    `json:"error"`
}

// SendRequest - структура тела запроса от прикладного уровеня
type SendRequest struct {
	Id       int       `json:"id,omitempty"`
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
}

// =====================================================================================================================
// STORAGE

type sendFunc func(body ReceiveRequest)
type Storage map[time.Time]Message

var storage = Storage{}

func addMessage(segment Segment) {
	storage[segment.SendTime] = Message{
		Received: 0,
		Total:    segment.TotalSegments,
		Last:     time.Now().UTC(),
		Username: segment.Username,
		Segments: make([]string, segment.TotalSegments), // заранее выделяем память, это важно!
	}
}

func AddSegment(segment Segment) {
	// используем мьютекс, чтобы избежать конкуретного доступа к хранилищу
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()

	// если это первый сегмент сообщения, создаем пустое сообщение
	sendTime := segment.SendTime
	_, found := storage[sendTime]
	if !found {
		addMessage(segment)
	}

	// добавляем в сообщение информацию о сегменте
	message, _ := storage[sendTime]
	message.Received++
	message.Last = time.Now().UTC()
	message.Segments[segment.SegmentNumber-1] = segment.SegmentPayload // сохраняем правильный порядок сегментов
	storage[sendTime] = message
}

func getMessageText(sendTime time.Time) string {
	result := ""
	message, _ := storage[sendTime]
	for _, segment := range message.Segments {
		result += segment
	}
	return result
}

func ScanStorage(sender sendFunc) {
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()

	payload := ReceiveRequest{}
	for sendTime, message := range storage {
		if message.Received == message.Total { // если пришли все сегменты
			payload = ReceiveRequest{
				Username: message.Username,
				Text:     getMessageText(sendTime), // склейка сообщения
				SendTime: sendTime,
				Error:    "",
			}
			fmt.Printf("sent message: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		} else if time.Since(message.Last) > KafkaReadPeriod+time.Second { // если канальный уровень потерял сегмент
			payload = ReceiveRequest{
				Username: message.Username,
				Text:     "",
				SendTime: sendTime,
				Error:    SegmentLostError, // ошибка
			}
			fmt.Printf("sent error: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		}
	}
}

// =====================================================================================================================
// KAFKA

func WriteToKafka(segment Segment) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// создание producer-а
	producer, err := sarama.NewSyncProducer([]string{KafkaAddr}, config)
	if err != nil {
		return fmt.Errorf("error creating producer: %w", err)
	}
	defer producer.Close()

	// превращение segment в сообщение для Kafka
	segmentString, _ := json.Marshal(segment)
	message := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Value: sarama.StringEncoder(segmentString),
	}

	// отправка сообщения
	_, _, err = producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func ReadFromKafka() error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// создание consumer-а
	consumer, err := sarama.NewConsumer([]string{KafkaAddr}, config)
	if err != nil {
		return fmt.Errorf("error creating consumer: %w", err)
	}
	defer consumer.Close()

	// подключение consumer-а к топика
	partitionConsumer, err := consumer.ConsumePartition(KafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("error opening topic: %w", err)
	}
	defer partitionConsumer.Close()

	// бесконечный цикл чтения
	for {
		select {
		case message := <-partitionConsumer.Messages():
			segment := Segment{}
			if err := json.Unmarshal(message.Value, &segment); err != nil {
				fmt.Printf("Error reading from kafka: %v", err)
			}
			fmt.Printf("%+v\n", segment) // выводим в консоль прочитанный сегмент
			AddSegment(segment)
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %s\n", err.Error())
		}
	}
}

// =====================================================================================================================
// SEGMENTATION

func SplitMessage(payload string, segmentSize int) []string {
	result := make([]string, 0)

	length := len(payload) // длина в байтах
	segmentCount := int(math.Ceil(float64(length) / float64(segmentSize)))

	for i := 0; i < segmentCount; i++ {
		result = append(result, payload[i*segmentSize:min((i+1)*segmentSize, length)]) // срез делается также по байтам
	}

	return result
}

func SendSegment(body Segment) {
	reqBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", CodeUrl, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()
}

func HandleSend(w http.ResponseWriter, r *http.Request) {
	// читаем тело запроса - сообщение
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// парсим сообщение в структуру
	message := SendRequest{}
	if err = json.Unmarshal(body, &message); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// сразу отвечаем прикладному уровню 200 ОК - мы приняли работу
	w.WriteHeader(http.StatusOK)

	// разбиваем текст сообщения на сегменты
	segments := SplitMessage(message.Text, SegmentSize)
	total := len(segments)

	// в цикле отправляем сегменты на канальный уровень
	for i, segment := range segments {
		payload := Segment{
			SegmentNumber:  i + 1,
			TotalSegments:  total,
			Username:       message.Username,
			SendTime:       message.SendTime,
			SegmentPayload: segment,
		}
		go SendSegment(payload) // запускаем горутину с отправкой на канальный уровень, не будем дожидаться результата ее выполнения
		fmt.Printf("sent segment: %+v\n", payload)
	}
}

// =====================================================================================================================
// DESEGMENTATION

func SendReceiveRequest(body ReceiveRequest) {
	reqBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", ReceiveUrl, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()
}

func HandleTransfer(w http.ResponseWriter, r *http.Request) {
	// читаем тело запроса - сегмент
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// парсим сегмент в структуру
	segment := Segment{}
	if err = json.Unmarshal(body, &segment); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// пишем сегмент в Kafka
	if err = WriteToKafka(segment); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// =====================================================================================================================
// MAIN

func main() {
	// запуск consumer-а
	go func() {
		if err := ReadFromKafka(); err != nil {
			fmt.Println(err)
		}
	}()

	// запуска сканирования хранилища
	go func() {
		ticker := time.NewTicker(KafkaReadPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ScanStorage(SendReceiveRequest)
			}
		}
	}()

	// создание роутера
	r := mux.NewRouter()

	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})

	r.HandleFunc("/send", HandleSend).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/transfer", HandleTransfer).Methods(http.MethodPost, http.MethodOptions)

	http.Handle("/", r)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// запуск http сервера
	srv := http.Server{
		Handler:           r,
		Addr:              ":8080",
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("Server stopped")
		}
	}()
	fmt.Println("Server started")

	// graceful shutdown
	sig := <-signalCh
	fmt.Printf("Received signal: %v\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown failed: %v\n", err)
	}
}
