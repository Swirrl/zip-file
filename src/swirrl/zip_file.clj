(ns swirrl.zip-file
  ""
  (:require [clojure.spec.alpha :as s]
            [datoteka.core :as fs])
  (:import [java.io ByteArrayInputStream]
           [java.net URI]
           [java.nio.file Files Path Paths FileSystems FileSystem CopyOption StandardCopyOption]
           [java.nio.file.attribute FileAttribute]))

(def local-fs "The default Local Filesystem" (FileSystems/getDefault))


(defprotocol IPaths
  (root-directories [fs])
  (*paths [fs depth path]))

(extend-protocol IPaths
  FileSystem
  (root-directories [fs]
    (seq (.getRootDirectories fs)))

  (*paths [fs depth path]
    (-> fs
        (.getPath (str path) (into-array String []))
        (java.nio.file.Files/walk depth (into-array java.nio.file.FileVisitOption []))
        .iterator
        iterator-seq)))

(defn paths
  "Returns a sequence of paths in the given file-system, by recursively
   walking the file system.

  If supplied with an fs and a path it will recursively list all paths
  beneath the given path.

  If supplied an fs a depth and a path it will recursively list all
  paths upto the specified depth."
  ([fs] (mapcat (partial paths fs) (root-directories fs)))
  ([fs path] (paths fs Integer/MAX_VALUE path))
  ([fs depth path]
   (*paths fs depth path)))

(defprotocol IPath
  (-path [fs pths]))

(extend-protocol IPath
  FileSystem
  (-path [fs pths]
    (if (seq pths)
      (.getPath fs (str (first pths)) (into-array String (fnext pths)))

      ;; hacky??
      (.getPath fs (str (first (sort (.getRootDirectories fs)))) (into-array String []))))

  String
  (-path [path more]
    (Paths/get (str path) (into-array String (map str more))))

  Path
  (-path [path more]
    (Paths/get (str path) (into-array String (map str more))))

  java.io.File
  (-path [path more]
    (-path local-fs (cons (str path) more))))

(defn path
  "Creates/coerces a java.nio.Path object out of various types
  supporting the IPath protocol.

  In particular if the first argument is a FileSystem object then the
  subsequent path will be coined within that FileSystem.
  "
  ([p] (-path p []))
  ([p & more]
   (-path p more)))

(defprotocol IInputStream
  (-input-stream [p opts]))

(extend-protocol IInputStream
  Path
  (-input-stream [p opts]
    (Files/newInputStream p (into-array java.nio.file.OpenOption opts))))

(defn input-stream
  "Coerce a path like thing to an input stream"
  ([p]
   (-input-stream p [])))

(defprotocol ICopyable
  (copyable [t]))

(extend-protocol ICopyable
  Path
  (copyable [p]
    p)

  String
  (copyable [s]
    (-> s .getBytes ByteArrayInputStream.))

  java.io.File
  (copyable [f]
    (fs/path (str f)))

  java.io.InputStream
  (copyable [ios]
    ios))


(defn create-directories [path]
  ;; TODO add opts arity
  (Files/createDirectories (fs/parent path) (into-array FileAttribute [])))

(defn open-zip-fs
  "Open a zip file at zip-file-path and return the java FileSystem
  object for it.  Users should do this in a with-open"
  [zip-file-path]
  (let [zf-uri (URI/create (str "jar:file:" zip-file-path))]
    (FileSystems/newFileSystem zf-uri {})))

(defn create-zip-fs [zip-file-path]
  ;; todo add opts arity
  (let [zf-uri (URI/create (str "jar:file:" (fs/normalize zip-file-path)))]
    (FileSystems/newFileSystem zf-uri {"create" "true"})))

(defn make-zip-file
  "Creates a zip file at zip-file-path with the contents of zip-spec
  written to it.

  A zip-spec is a map of zipable-sources to their paths in the zip."
  [zip-file-path zip-spec]
  (with-open [zipfs (create-zip-fs zip-file-path)]

    (doseq [[dest-file src-content] zip-spec]
      (let [dest-in-zip (path zipfs dest-file)]

        ;; create any parent paths to destination
        (create-directories dest-in-zip)
        (Files/copy (copyable src-content)
                    dest-in-zip (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))))

    zipfs))

(s/def ::path #(instance? Path %))

(s/def ::zipable-source #(satisfies? ICopyable %))

(s/def ::zip-spec (s/map-of ::zipable-source (s/or :path ::path
                                                   :content ::zipable-source)))

(s/def ::zip-file-system #(instance? FileSystem %))

(s/fdef make-zip-file
  :args (s/cat :zip-file-path ::path
               :zip-spec ::zip-spec)
  :ret ::zip-file-system)
