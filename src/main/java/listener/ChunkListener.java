package listener;

import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.AfterChunkError;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.scope.context.ChunkContext;

public class ChunkListener {

    @BeforeChunk
    public void beforeChunk(ChunkContext context) {
        System.out.println(">> Before the chunk, thread:" + Thread.currentThread().getName());
    }

    @AfterChunk
    public void afterChunk(ChunkContext context) {
        System.out.println("<< After the chunk, thread:" + Thread.currentThread().getName());
    }

    @AfterChunkError
    public void afterChunkError(ChunkContext chunkContext) {
        System.out.println(">> AfterChunkError StepExecution Listener Thread:" + Thread.currentThread().getName() + ", Object hashcode:" + this);
    }
}
