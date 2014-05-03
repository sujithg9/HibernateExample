package com.hibernate.employeebean;

import java.io.Serializable;
import java.util.Iterator;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;

public class MyInterceptor extends EmptyInterceptor {
   /**
	 * 
	 */
private static final long serialVersionUID = 1L;
   @SuppressWarnings("unused")
   private int updates;
   @SuppressWarnings("unused")
private int creates;
   @SuppressWarnings("unused")
   private int loads;

   public void onDelete(Object entity,
                     Serializable id,
                     Object[] state,
                     String[] propertyNames,
                     Type[] types) {
       // do nothing
   }

   // This method is called when Employee object gets updated.
   public boolean onFlushDirty(Object entity,
                     Serializable id,
                     Object[] currentState,
                     Object[] previousState,
                     String[] propertyNames,
                     Type[] types) {
       if ( entity instanceof Employee ) {
          System.out.println("Update Operation");
          return true; 
       }
       return false;
   }
   public boolean onLoad(Object entity,
                    Serializable id,
                    Object[] state,
                    String[] propertyNames,
                    Type[] types) {
       // do nothing
       return true;
   }
   // This method is called when Employee object gets created.
   public boolean onSave(Object entity,
                    Serializable id,
                    Object[] state,
                    String[] propertyNames,
                    Type[] types) {
       if ( entity instanceof Employee ) {
          System.out.println("Create Operation");
          return true; 
       }
       return false;
   }
   //called before commit into database
   public void preFlush(@SuppressWarnings("rawtypes") Iterator iterator) {
      System.out.println("preFlush");
   }
   //called after committed into database
   @SuppressWarnings("rawtypes")
public void postFlush(Iterator iterator) {
      System.out.println("postFlush");
   }
}