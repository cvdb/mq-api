package com.transixs.mq;

import java.lang.reflect.InvocationTargetException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkerManager {

  private static final Logger log = LogManager.getLogger(WorkerManager.class.getName());

  // We want to ensure that all Worker classes use the required
  // annotations and that each annotated method has the required signature
  //
  public static void validateWorkerFactory(IWorkerFactory factory) {
    for (Class c : factory.getWorkerClasses()) {
      // We expect the worker class to be annotated with a RESOURCE
      if (!hasResourceAnotation(c)) {
        throw new InvalidWorkerException("worker is not annotated with 'Resource'");
      }
      // We expect at least one method to be anotated with 'Action' and
      // it should return WorkResult type and accpet a single STRING parameter
      if (!hasActionMethod(c)) {
        throw new InvalidWorkerException("worker does not have a valid action method");
      }
    }
  }

  public static WorkResult doWork(String resource, String action, IWorkerFactory workerFactory, String content) {
    //NOTE: this assumes only 1 class per resource
    Class workerClass = getResourceClass(workerFactory.getWorkerClasses(), resource);
    Object worker = workerFactory.getWorker(workerClass);
    Method m = getActionMethod(workerClass, action);
    if (m == null) {
      throw new InvalidWorkerException("failed to find worker for resource:" + resource + " and action:" + action);
    }
    return doWork(worker, m, content);
  }

  private static WorkResult doWork(Object w, Method m, String content) {
    try {
      m.setAccessible(true);
      Object o = m.invoke(w, content);
      return (WorkResult)o;
    } catch (Exception e) {
      throw new InvalidWorkerException("worker does not have a valid action method", e);
    }
  }

  private static boolean hasActionMethod(Class c) {
    for (Method m : c.getDeclaredMethods()) {
      if (isActionMethod(m)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isActionMethod(Method m) {
    if (!hasActionAnotation(m)) {
      return false;
    }
    if (m.getReturnType() != WorkResult.class) {
      return false;
    }
    Class<?>[] pType  = m.getParameterTypes();
    if (pType.length != 1 || pType[0] != String.class) {
      return false;
    }
    return true;
  }

  // At this point validation has already been done
  private static Method getActionMethod(Class c, String action) {
    for (Method m : c.getDeclaredMethods()) {
      if (!matchesActionAnotation(m, action)) {
        continue;
      } else {
        return m;
      }
    }
    return null;
  }

  // At this point validation has already been done
  private static Class getResourceClass(List<Class> classes, String resource) {
    for(Class c : classes) {
      if (matchesResourceAnotation(c, resource)) {
        return c;
      }
    }
    return null;
  }

  private static boolean hasResourceAnotation(AnnotatedElement element) {
    try {
      Annotation[] annotations = element.getAnnotations();
      for (Annotation annotation : annotations) {
        if (annotation instanceof Resource) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      log.error("failed to check for 'Resource' annotation", e);
      return false;
    }
  }

  private static boolean matchesResourceAnotation(AnnotatedElement element, String resource) {
    try {
      Annotation[] annotations = element.getAnnotations();
      for (Annotation annotation : annotations) {
        if (annotation instanceof Resource) {
          Resource resourceAnnotation = (Resource) annotation;
          if (resourceAnnotation.value().equals(resource)) {
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      log.error("failed to match for 'Resource' annotation", e);
      return false;
    }
  }

  private static boolean hasActionAnotation(AnnotatedElement element) {
    try {
      Annotation[] annotations = element.getAnnotations();
      for (Annotation annotation : annotations) {
        if (annotation instanceof Action) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      log.error("failed to check for 'Action' annotation", e);
      return false;
    }
  }

  private static boolean matchesActionAnotation(AnnotatedElement element, String action) {
    try {
      Annotation[] annotations = element.getAnnotations();
      for (Annotation annotation : annotations) {
        if (annotation instanceof Action) {
          Action actionAnnotation = (Action) annotation;
          if (actionAnnotation.value().equals(action)) {
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      log.error("failed to match for 'Action' annotation", e);
      return false;
    }
  }

}
