(ns cassandra.lwt-test
  (:require [clojure.test :refer :all]
            [cassandra.lwt :refer :all]
            [cassandra.core-test :refer :all]))


;; Steady state cluster tests                              
(deftest ^:lwt ^:steady lwt-bridge                         
  (run-test! bridge-test))                                 
                                                           
(deftest ^:lwt ^:steady lwt-isolate-node                   
  (run-test! isolate-node-test))                           
                                                           
(deftest ^:lwt ^:steady lwt-halves                         
  (run-test! halves-test))                                 
                                                           
(deftest ^:lwt ^:steady lwt-crash-subset                   
  (run-test! crash-subset-test))                           
                                                           
(deftest ^:lwt ^:steady lwt-flush-compact                  
  (run-test! flush-compact-test))                          
                                                           
;(deftest ^:clock lwt-clock-drift                          
;  (run-test! clock-drift-test))                           
                                                           
;; Bootstrapping tests                                     
(deftest ^:lwt ^:bootstrap lwt-bridge-bootstrap            
  (run-test! bridge-test-bootstrap))                       
                                                           
(deftest ^:lwt ^:bootstrap lwt-isolate-node-bootstrap      
  (run-test! isolate-node-test-bootstrap))                 
                                                           
(deftest ^:lwt ^:bootstrap lwt-halves-bootstrap            
  (run-test! halves-test-bootstrap))                       
                                                           
(deftest ^:lwt ^:bootstrap lwt-crash-subset-bootstrap      
  (run-test! crash-subset-test-bootstrap))                 
                                                           
;(deftest ^:clock lwt-clock-drift-bootstrap                
;  (run-test! clock-drift-test-bootstrap))                 
                                                           
;; Decommission tests                                      
(deftest ^:lwt ^:decommission lwt-bridge-decommission      
  (run-test! bridge-test-decommission))                    
                                                           
(deftest ^:lwt ^:decommission lwt-isolate-node-decommission
  (run-test! isolate-node-test-decommission))              
                                                           
(deftest ^:lwt ^:decommission lwt-halves-decommission      
  (run-test! halves-test-decommission))                    
                                                           
(deftest ^:lwt ^:decommission lwt-crash-subset-decommission
  (run-test! crash-subset-test-decommission))              
                                                           
;(deftest ^:clock lwt-clock-drift-decommission             
;  (run-test! clock-drift-test-decommission))              
