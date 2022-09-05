package br.com.giulianabezerra.sbrestart;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Bean
  public Job job(Step step) {
    return jobBuilderFactory
        .get("job")
        .start(step)
        .build();
  }

  @Bean
  public Step step(ItemReader<String> reader, ItemProcessor<String, String> processor, ItemWriter<String> writer) {
    return stepBuilderFactory
        .get("step")
        .<String, String>chunk(10)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
  }

  @Bean
  public ItemReader<String> reader() {
    return new FlatFileItemReaderBuilder<String>()
        .name("reader")
        .resource(new FileSystemResource("files/numeros.csv"))
        .lineMapper((lineMapper()))
        .build();
  }

  private LineMapper<String> lineMapper() {
    return new LineMapper<String>() {
      @Override
      public String mapLine(String line, int lineNumber) throws Exception {
        return line;
      }
    };
  }

  @Bean
  public ItemProcessor<String, String> processor() {
    return item -> {
      // Adiciona erro!
      if (item.equals("51"))
        throw new RuntimeException("Deu erro!");

      return item;
    };
  }

  @Bean
  public ItemWriter<String> writer() {
    return System.out::println;
  }
}
