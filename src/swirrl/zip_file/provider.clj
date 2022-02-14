(ns swirrl.zip-file.provider
  "This is necessary because the java provider maintains a registry of
  open filesystems and forces consumers to either call newFileSystem
  to create a new file system for a specified zip, or to call
  getFileSystem if newFileSystem has already been called.

  The Java zip file system will then release the reference to created
  file systems on close. For us, when accessing multiple zips this is
  a race condition, as two users reading out of the same zip may close
  the file-sytem before the other user has finished reading it.

  So we wrap the underlying java code behind our own abstraction.

  Our abstraction simply maintains a registry of
  AccessTrackingFileSystem objects, which store an additional
  access-count against the file-system.

  When concurrent users acquire-fs we incrememnt the access-count
  associated with the file-system, and decrement it on calls to
  `.close`, only when the access-count is zero do we actually `.close`
  the underlying zip filesystem and remove the mapping from our
  registry.

  Correct usage of this is inside a `with-open`:

  (with-open [fs (acquire-fs ,,,)]
    ,,,)"
  (:require [swirrl.zip-file :as zip-f])
  (:import [java.net URI]
           [java.nio.file FileSystems]))


(def ^:private file-systems
  "Registry of concurrently accessed file systems. An atom over a Map
  from a URI identifying the ZipFileSystem to an
  AccessTrackingFileSystem object."
  (atom {}))

(def ^:private fs-pool-monitor
  "Monitor to establish a mutex over the pool of zip files being
  accessed."
  (Object.))

(defn- inc-access-count
  "Increment the access count on an AccessTrackingFileSystem object."
  [pooled-fs]
  (update pooled-fs :access-count inc))

(defn- dec-access-count
  "Decrement the access count on an AccessTrackingFileSystem object."
  [pooled-fs]
  (update pooled-fs :access-count dec))

(defrecord AccessTrackingFileSystem [fs-uri zip-fs access-count]
  zip-f/IPaths
  (root-directories [fs]
    (zip-f/root-directories zip-fs))

  (*paths [_fs depth path]
    (zip-f/*paths zip-fs depth path))

  zip-f/IPath
  (-path [fs pths]
    (zip-f/-path zip-fs pths))


  java.io.Closeable
  (close [_]
    (locking fs-pool-monitor
      (let [access-count (let [systems (swap! file-systems update fs-uri dec-access-count)]
                           (:access-count (get systems fs-uri)))]
        (when (zero? access-count)
          (.close zip-fs)
          (swap! file-systems dissoc fs-uri)))
      nil)))

(defn acquire-fs
  "Synchronise access to the underlying mutable java Zip FileSystem
  provider. Use this to safely read the contents of a zip file system
  from multiple threads, without race conditions or memory leaks.
  Works around issues in the java implementation.

  Use in conjunction with `.close` i.e. inside a `with-open`."
  [zip-file-path]
  (locking fs-pool-monitor
    (let [zf-uri (URI/create (str "jar:file:" zip-file-path))]
      (if (get @file-systems zf-uri)
        (get (swap! file-systems update zf-uri inc-access-count)
             zf-uri)
        (let [fs (FileSystems/newFileSystem zf-uri {})
              tracking-fs (->AccessTrackingFileSystem zf-uri fs 1)]
          (swap! file-systems assoc zf-uri tracking-fs)
          tracking-fs)))))


(comment

  (do

    (def fs (acquire-fs "/Users/rick/repos/muttnik/muttnik-gov-simple-hierarchy-data-12.zip"))
    (def fs2 (acquire-fs "/Users/rick/repos/muttnik/muttnik-gov-simple-hierarchy-data-12.zip"))
    (def fs3 (acquire-fs "/Users/rick/repos/muttnik/muttnik-gov-simple-hierarchy-data-12.zip"))

    (.close fs)
    (.close fs2)
    (.close fs3)

    )


  )
