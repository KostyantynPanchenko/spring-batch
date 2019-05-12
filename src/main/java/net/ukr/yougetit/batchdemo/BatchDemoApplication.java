package net.ukr.yougetit.batchdemo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;

@EnableBatchProcessing
@SpringBootApplication
public class BatchDemoApplication {

    @Bean
    Job job(final JobBuilderFactory jbf,
            final StepBuilderFactory sbf,
            final Step1Configuration step1Configuration,
            final Step2Configuration step2Configuration) {

        return jbf.get("etl")
                .incrementer(new RunIdIncrementer())
                .start(step1(sbf, step1Configuration))
                .next(step2(sbf, step2Configuration))
                .build();
    }

    @Bean
    Step step1(final StepBuilderFactory sbf, final Step1Configuration step1Configuration) {
        return sbf.get("file-to-db")
                .<Person, Person> chunk(10)
                .reader(step1Configuration.fileReader(null))
                .writer(step1Configuration.jdbcWriter(null))
                .build();
    }

    @Configuration
    public static class Step1Configuration {

        @Bean
        FlatFileItemReader<Person> fileReader(@Value("${inputFile}") final String filePath) {
        final Resource input = new ClassPathResource(filePath);
            return new FlatFileItemReaderBuilder<Person>()
                    .name("file-reader")
                    .resource(input)
                    .targetType(Person.class)
                    .delimited().delimiter(",").names(new String[]{"firstName", "age", "email"})
                    .build();
        }

        @Bean
        JdbcBatchItemWriter<Person> jdbcWriter(final DataSource dataSource) {
            return new JdbcBatchItemWriterBuilder<Person>()
                    .dataSource(dataSource)
                    .sql("INSERT INTO people(age, first_name, email) VALUES(:age, :firstName, :email)")
                    .beanMapped()
                    .build();
        }
    }

    @Bean
    Step step2(final StepBuilderFactory sbf, final Step2Configuration step2Configuration) {
        return sbf.get("db-to-file")
                .<Map<Integer, Integer>, Map<Integer, Integer>> chunk(10)
                .reader(step2Configuration.jdbcReader(null))
                .writer(step2Configuration.fileWriter(null))
                .build();
    }

    @Configuration
    public static class Step2Configuration {

        @Bean
        ItemReader<Map<Integer, Integer>> jdbcReader(final DataSource dataSource) {
            return new JdbcCursorItemReaderBuilder<Map<Integer, Integer>>()
                    .name("jdbc-reader")
                    .dataSource(dataSource)
                    .sql("SELECT COUNT(age) AS count, age FROM people GROUP BY age")
                    .rowMapper((ResultSet rs, int i) ->  Collections.singletonMap(rs.getInt("age"), rs.getInt("count")))
                    .build();
        }

        @Bean
        ItemWriter<Map<Integer, Integer>> fileWriter(@Value("${outputFile:}") final String filePath) {
            final Resource output = new FileSystemResource(filePath);
            return new FlatFileItemWriterBuilder<Map<Integer, Integer>>()
                    .name("file-writer")
                    .resource(output)
                    .lineAggregator(new DelimitedLineAggregator<Map<Integer, Integer>>() {
                        {
                            setDelimiter(",");
                            setFieldExtractor(integerIntegerMap -> {
                                Map.Entry<Integer, Integer> next = integerIntegerMap.entrySet().iterator().next();
                                return new Object[]{next.getKey(), next.getValue()};
                            });
                        }
                    })
                    .build();
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(BatchDemoApplication.class, args);
    }

}
