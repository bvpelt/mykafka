package nl.bsoft.kafka.mydemo;

import kafka.utils.Time;

public class TestTime implements Time {

	 @Override 
     public long milliseconds() { 
       return System.currentTimeMillis(); 
     } 

     @Override 
     public long nanoseconds() { 
       return System.nanoTime(); 
     } 

     @Override 
     public void sleep(long ms) { 
       try { 
         Thread.sleep(ms); 
       } catch (InterruptedException e) { 
         Thread.interrupted(); 
       } 
     }

}
