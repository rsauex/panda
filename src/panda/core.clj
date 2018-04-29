(ns panda.core
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [aleph.udp :as udp]
    [aleph.tcp :as tcp]
    [clojure.string :as str]
    [byte-streams :as bs]
    [clojure.core.async :as a
     :refer [>! <! >!! <!! go chan buffer close! alts! alts!! timeout]]
    [gloss.core :as g]
    [gloss.io :as io])
  (:import
   [java.net SocketAddress InetSocketAddress InetAddress])
  (:gen-class))

;;; We have two queues:

;; for tasks to be sent
(def tasks-chan (chan 20))
;; for results received from clients
(def results-chan (chan 20))

;; UDP port for receiving connection requests
(def udp-port 48655)

(def m-size 1000)

(g/defcodec m-msg-f
  [:int32-be (g/repeated :int32-be) (g/repeated :int32-be)])

(g/defcodec m-resp-f
  [:int32-be (g/repeated :int32-be)])

(defn safe-println [& more]
  (.write *out* (str (clojure.string/join " " more) "\n"))
  (flush))

(defn wrap-duplex-clnt-stream [s]
  (let [out (s/stream)]
    (s/connect (s/map #(io/encode m-msg-f %) out) s)
    (s/splice out (io/decode-stream s m-resp-f))))

(defn check-result [r message]
  (when (or (nil? r)
            (= ::none r))
    (throw (ex-info message {})))
  r)

(defn add-client [address]
  (d/catch (d/let-flow [socket (d/chain
                                ;; Connect to client
                                (tcp/client {:remote-address address})
                                ;; Wrap
                                #(wrap-duplex-clnt-stream %))]
             ;; client loop
             (d/loop []
               ;; Get next task
               (let [task (<!! tasks-chan)]
                 (d/catch (d/chain
                           task
                           ;; Send task to client
                           #(s/put! socket %)
                           #(check-result % (str "Disconnected: " address))
                           ;; Receive result
                           (fn [_] (s/take! socket ::none))
                           #(check-result % (str "Disconnected: " address))
                           ;; If successful put result into results channel
                           ;; else put task back to tasks channel
                           #(>!! results-chan %)
                           (fn [_] (d/recur)))
                     (fn [ex]
                       (safe-println "Error: " ex)
                       (>!! tasks-chan task)
                       (s/close! socket))))))
      (fn [ex]
        (safe-println "Error: " ex))))

(defn parse-connection-packet
  "Returns an address for tcp connection to client for
a given connection request message from client."
  [msg]
  (let [host (.getAddress ^InetSocketAddress (:sender msg))
        port (io/decode (g/compile-frame :int32-be) (:message msg))]
    (InetSocketAddress. ^InetAddress host port)))

(defn log-new-connection [address]
  (safe-println (str "New connection: " address))
  address)

(defn start-connection-server
  "Creates a connection server which accepts connection requests
from clients and starts connection with client."
  []
  (safe-println "Start connection server")
  (->> @(udp/socket {:port udp-port})
       (s/map parse-connection-packet)
       (s/map log-new-connection)
       (s/consume add-client)))

;; Matrix

(defn make-matrix [size]
  (loop [i 0, res (transient [])]
    (if (< i size)
      (recur (inc i) (conj! res (repeatedly size #(rand-int 32000))))
      (persistent! res))))

(defn tasks []
  (let [m1 (make-matrix m-size)
        m2 (make-matrix m-size)]
    (safe-println "Matrix 1:")
    (safe-println m1)
    (safe-println "\nMatrix 2:")
    (safe-println m2)
    (safe-println " ")
    (loop [i 0, m1 m1, m2 m2]
      (>!! tasks-chan [i (first m1) (first m2)])
      (when (next m1)
        (recur (inc i) (next m1) (next m2))))))

(defn collect-result []
  (loop [i 0, res (transient (vec (repeat m-size nil)))]
    (if (= i m-size)
      (do (safe-println "Result")
          (persistent! res))
      (let [[t-num r] (<!! results-chan)]
        (recur (inc i) (assoc! res t-num r))))))

(defn -main
  [& args]
  (go (start-connection-server))
  (loop []
    (go (tasks))
    (collect-result)
    ;;(recur)
    ))

;;; Client

(defn procs [x]
  (safe-println "Got Task")
  (let [[t-num r1 r2] x]
    [t-num (mapv + r1 r2)]))

(defn wrap-duplex-stream [s]
  (let [out (s/stream)]
    (s/connect (s/map #(io/encode m-resp-f %) out) s)
    (s/splice out (io/decode-stream s m-msg-f))))

(defn start-clnt []
  (let [server (tcp/start-server
                (fn [s _]
                  (let [sout (wrap-duplex-stream s)]
                    (s/connect (s/map procs sout) sout)))
                {:port 0})
        port (aleph.netty/port server)]
    (safe-println "Clnt started: " port)
    (let [s-socket @(udp/socket {:broadcast? true})]
      (s/put! s-socket
              {:host "255.255.255.255"
               :port udp-port
               :message (-> (java.nio.ByteBuffer/allocate 4)
                            (.order java.nio.ByteOrder/BIG_ENDIAN)
                            (.putInt port)
                            .array)}))))

;; (defn -main
;;   "I don't do a whole lot ... yet."
;;   [& args]
;;   (start-clnt))
