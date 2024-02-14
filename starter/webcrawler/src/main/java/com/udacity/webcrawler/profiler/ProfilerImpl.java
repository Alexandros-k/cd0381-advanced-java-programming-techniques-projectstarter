package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Handler;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();
  private final ZonedDateTime startTime;

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    this.startTime = ZonedDateTime.now(clock);
  }

  @Override
  public <T> T wrap(Class<T> klass, T delegate) {
    Objects.requireNonNull(klass);
    boolean containsProfiledMethod = Arrays.stream(klass.getMethods())
            .anyMatch(method -> method.isAnnotationPresent(Profiled.class));

    if (!containsProfiledMethod) {
      throw new IllegalArgumentException("The wrapped interface does not contain a @Profiled method.");
    }
    // TODO: Use a dynamic proxy (java.lang.reflect.Proxy) to "wrap" the delegate in a
    //       ProfilingMethodInterceptor and return a dynamic proxy from this method.
    //       See https://docs.oracle.com/javase/10/docs/api/java/lang/reflect/Proxy.html.

    Object proxy = Proxy.newProxyInstance(klass.getClassLoader(),
            new Class[]{klass},
            new ProfilingMethodInterceptor(clock, state, delegate)
            );

    return (T)proxy;
  }

  @Override
  public void writeData(Path path) {
    // TODO: Write the ProfilingState data to the given file path. If a file already exists at that
    //       path, the new data should be appended to the existing file.
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      // Write a header indicating the start time
      writer.write("Data recorded since: " + startTime.format(DateTimeFormatter.ISO_DATE_TIME));
      writer.newLine();

      // Write the method invocation data
      state.getData().forEach((methodKey, duration) -> {
        try {
          writer.write(methodKey);
          writer.newLine();
        } catch (IOException e) {
          e.printStackTrace(); // Handle the exception appropriately
        }
      });
    } catch (IOException e) {
      e.printStackTrace(); // Handle the exception appropriately
    }

  }

  @Override
  public void writeData(Writer writer) throws IOException {
    writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
    writer.write(System.lineSeparator());
    state.write(writer);
    writer.write(System.lineSeparator());
  }
}
