package main
import ("context";"fmt";"log";"time";"github.com/segmentio/kafka-go")
func main(){
conn,_:=kafka.Dial("tcp","kafka-gateway:9092");c,_:=conn.Controller();conn.Close()
cc,_:=kafka.Dial("tcp",c.Host);cc.CreateTopics(kafka.TopicConfig{Topic:"extract",NumPartitions:1,ReplicationFactor:1});cc.Close();time.Sleep(2*time.Second)
w:=&kafka.Writer{Addr:kafka.TCP("kafka-gateway:9092"),Topic:"extract"};d:="{\"fix\":\"unsigned_varint\"}";ctx,_:=context.WithTimeout(context.Background(),10*time.Second)
w.WriteMessages(ctx,kafka.Message{Key:[]byte("k1"),Value:[]byte(d)});w.Close();fmt.Printf("✅ Produced: %s\n",d);time.Sleep(2*time.Second)
r:=kafka.NewReader(kafka.ReaderConfig{Brokers:[]string{"kafka-gateway:9092"},Topic:"extract",GroupID:"test"});rctx,_:=context.WithTimeout(context.Background(),10*time.Second)
m,e:=r.ReadMessage(rctx);r.Close();if e!=nil{log.Fatal(e)};fmt.Printf("✅ Consumed: %s\n",m.Value);if string(m.Value)==d{fmt.Println("\n🎉 EXTRACTION WORKS!")}}
