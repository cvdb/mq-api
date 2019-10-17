package com.transixs.mq;

import java.util.List;

/*******************************************************************
 * NOTE: The GOAL here is to make the MQ API as simple and easy to use
 * as posible BUT keep it very light. We don't want to use a DI framework.
 *
 * The list of worker classes returned must be annotated
 * with 'Resource' and 'Action's. At runtime the target worker
 * will be identified from the class list based on resource and action.
 * This factory will then be called to construct the required worker.
 * The matching method with 'Action' anotation will be called
 *******************************************************************/


public interface IWorkerFactory {

  public Object getWorker(Class clazz);

  public List<Class> getWorkerClasses();

}
