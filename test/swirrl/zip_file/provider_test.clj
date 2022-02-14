(ns swirrl.zip-file.provider-test
  (:require [clojure.test :as t]
            [datoteka.core :as fs]
            [swirrl.zip-file.provider :as sut]
            [swirrl.zip-file :as zip]
            ))

(defmacro with-temp-file
  "Create a temp file and bind it to the supplied symbol, then clean the
  file up afterwards."
  [tf & body]
  `(let [~tf (fs/create-tempfile)
         ret# (do ~@body)]

     (fs/delete ~tf)
     ret#))

(defmacro with-temp-dir
  "Create a temp dir and bind it to the supplied symbol, then clean the
  directory and everything within it afterwards."
  [td & body]
  `(let [fname# (fs/path "/tmp/cis-loader" (str (gensym)))]
     (try
       (let [~td (fs/create-dir fname#)]
         ~@body)
       (finally
         (fs/delete fname#)))))


(t/deftest concurrent-access-simulation
  ;; This test simulates concurrent accesses on the same zip file with
  ;; an interleaving of operations that would under direct use of the
  ;; java filesystem API lead to errors and exceptions being raised.
  ;;
  ;; Here we are testing orchestration of the Java File system library
  ;; via our provider abstraction.
  (with-temp-dir td
    (let [zip-file (zip/path td "foo.zip")]
      (zip/make-zip-file zip-file
                         {(zip/path "/foo/bar.txt") "This is bar.txt!"})

      (let [zf1 (sut/acquire-fs zip-file)
            zf2 (sut/acquire-fs zip-file)
            path1 (zip/path zf1 "/foo/bar.txt")
            path2 (zip/path zf2 "/foo/bar.txt")

            contents1 (slurp (zip/input-stream path1))
            _ (.close zf1)

            ;; ^-- here we close zf1, if the acquisitions weren't
            ;; tracked properly on the underlying shared zip file
            ;; system object the next line wouldn't be able to read
            ;; path2 from zf2

            contents2 (slurp (zip/input-stream path2))
            _ (.close zf2)]


        (t/is (= "This is bar.txt!" contents1 contents2)
              "Read different contents from the same zip/path")))))
