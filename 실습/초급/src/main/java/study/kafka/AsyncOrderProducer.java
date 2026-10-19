package study.kafka;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.*;

/** Bounded in-flight application sends with async result observation. */
public final class AsyncOrderProducer {
    public static void run(String[] args) throws Exception {
        int count=Settings.number(args,0,100);
        String topic=Settings.arg(args,1,"orders");
        String run=Settings.arg(args,2,Settings.runId());
        var permits=new Semaphore(32);
        var firstFailure=new AtomicReference<Exception>();
        var p=Settings.producerProperties();
        p.put("linger.ms","5");
        p.put("batch.size","32768");
        p.put("compression.type","lz4");
        try(var producer=new KafkaProducer<String,String>(p)) {
            // region async-send
            for(int i=0;i<count;i++) {
                if(firstFailure.get()!=null) break;
                if(!permits.tryAcquire(20,TimeUnit.SECONDS)) throw new TimeoutException("Application send queue is full");
                try {
                    OrderEvent event=OrderEvent.create(run+"-"+i,"order-"+(i%3),1000);
                    producer.send(new ProducerRecord<>(topic,event.orderId(),event.json()),(metadata,error)->{
                        try {
                            if(error!=null) firstFailure.compareAndSet(null,error);
                            else System.out.printf("ASYNC_ACK event=%s partition=%d offset=%d%n",event.eventId(),metadata.partition(),metadata.offset());
                        } finally { permits.release(); }
                    });
                } catch(Exception error) {
                    // A synchronous failure does not invoke the callback.
                    permits.release();
                    throw error;
                }
            }
            producer.flush();
            if(firstFailure.get()!=null) throw firstFailure.get();
            // endregion async-send
        }
    }
}
