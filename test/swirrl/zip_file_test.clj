(ns swirrl.zip-file-test
  (:require [swirrl.zip-file :as sut]
            [clojure.test :as t]
            [datoteka.core :as fs]
            [clojure.string :as str])
  (:import [java.nio.file Path Paths Files FileSystems FileSystem]))

(defn normalise-path-across-jdks
  "JDK 8 prints directory paths in zips with a trailing slash.  JDK9+ doesn't.

  e.g. JDK8 prints /foo/
  and JDK9+ prints /foo

  We can remove this function when we target JDK11+ in all environments"
  [str]
  (if (= "/" str)
    str
    (str/replace str #"/$" "")))

(defn slurp-path [path]
  (slurp (sut/input-stream path)))

(t/deftest test-zip-file-creation
  (let [zip-file (sut/path (str "/tmp/" (gensym "mut-test") ".zip"))

        readme-path (sut/path "/README.txt")
        file-in-directory-path (sut/path "/somewhere/in-a-directory.txt")
        _zfs (sut/make-zip-file zip-file
                                {readme-path "README content"
                                 file-in-directory-path "A file in a directory"})

        zfs (sut/open-zip-fs (str zip-file))
        files-in-zip (sut/paths zfs)]

    (t/is (= #{"/" "/README.txt" "/somewhere" "/somewhere/in-a-directory.txt"}
             (set (->> files-in-zip
                       (map str)
                       (map normalise-path-across-jdks)))))

    (t/is (= "README content" (slurp-path (sut/path zfs readme-path))))
    (t/is (= "A file in a directory" (slurp-path (sut/path zfs file-in-directory-path))))


    (fs/delete zip-file)))
