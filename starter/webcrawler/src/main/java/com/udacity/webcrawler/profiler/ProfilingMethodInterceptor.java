package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final ProfilingState state;
  private final Object delegate;

  Instant end;

  // TODO: You will need to add more instance fields and constructor arguments to this class.
  ProfilingMethodInterceptor(Clock clock, ProfilingState state, Object delegate) {
      this.delegate = Objects.requireNonNull(delegate);
    this.clock = Objects.requireNonNull(clock);
      this.state =  Objects.requireNonNull(state);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // TODO: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.
    if (method.isAnnotationPresent(Profiled.class)) {
       Instant start = clock.instant();
     try {
       Object invoke = method.invoke(delegate, args);
       Instant end = clock.instant();
       Duration duration = Duration.between(start, end);
       state.record(delegate.getClass(), method, duration);
       return invoke;
     }catch (InvocationTargetException  e){
        end = clock.instant();
       Duration duration = Duration.between(start, end);
       state.record(delegate.getClass(), method, duration);
       throw e.getTargetException();
     }
    }else if (method.getName().equals("toString")) {
      return delegate.toString();
    }else if (method.getName().equals("hashCode")) {
      return delegate.hashCode();
    }else if (method.getName().equals("equals") && args != null && args.length == 1) {
      return delegate.equals(args[0]);
    }else if (method.getName().equals("Throwable")) {
      return delegate.equals(delegate);
    }else {
      return method.invoke(delegate, args);
    }
  }
}
