-- {-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | an unsafe interface to CouchDB.  Database and document names are not
-- sanitized.
module Database.CouchDB.Unsafe
  (
  -- * Databases
    createDB
  , dropDB
  , getAllDBs
  -- * Documents
  , newNamedDoc
  , newDoc
  , updateDoc
  , bulkUpdateDocs
  , deleteDoc
  , forceDeleteDoc
  , getDocPrim
  , getDocRaw
  , getDoc
  , getAndUpdateDoc
  , getAllDocIds
  , getAllDocs
  -- * Views
  -- $views
  , CouchView (..)
  , newView
  , queryView
  , queryViewKeys
  ) where

import Database.CouchDB.HTTP
import Control.Monad
import Control.Monad.Trans (liftIO)
import Data.Maybe (fromJust, mapMaybe, isNothing)
import Data.Aeson
import qualified Data.HashMap.Strict as M
import qualified Data.Vector as V
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as LB
import qualified Data.List as L

fromObject :: Object -> [(T.Text,Value)]
fromObject obj =  M.toList obj

assertObject :: Value -> CouchMonad Value
assertObject v@(Object _) = return v
assertObject o = fail $ "expected a JSON object; received: " ++ (LB.unpack $ encode o)

couchResponse :: String -> [(String,Value)]
couchResponse respBody = case decode . LB.pack $ respBody of
  Nothing -> error $ "couchResponse invalid."
  Just r -> map (\(k,v) -> (T.unpack k,v)) $ fromObject r

request' :: String -> RequestMethod -> CouchMonad (Response String)
request' path method = request path [] method [] ""

-- |Creates a new database.  Throws an exception if the database already
-- exists.
createDB :: String -> CouchMonad ()
createDB name = do
  resp <- request' name PUT
  unless (rspCode resp == (2,0,1)) $
    error (rspReason resp)

dropDB :: String -> CouchMonad Bool -- ^False if the database does not exist
dropDB name = do
  resp <- request' name DELETE
  case rspCode resp of
    (2,0,0) -> return True
    (4,0,4) -> return False
    otherwise -> error (rspReason resp)

getAllDBs :: CouchMonad [String]
getAllDBs = do
  response <- request' "_all_dbs" GET
  case rspCode response of
    (2,0,0) ->
      case decode $ LB.pack (rspBody response) of
        Just (Array dbs) -> return [T.unpack db | String db <- V.toList dbs]
        Nothing          -> error $ "Unexpected couch response."
    otherwise -> error (show response)

newNamedDoc :: (ToJSON a)
            => String -- ^database name
            -> String -- ^document name
            -> a -- ^document body
            -> CouchMonad (Either String String)
            -- ^Returns 'Left' on a conflict.  Returns 'Right' with the
            -- revision number on success.
newNamedDoc dbName docName body = do
  obj <- assertObject (toJSON body)
  r <- request (dbName ++ "/" ++ docName) [] PUT [] (LB.unpack $ encode obj)
  case rspCode r of
    (2,0,1) -> do
      let result = couchResponse (rspBody r)
      let (String rev) = fromJust $ lookup "rev" result
      return (Right $ T.unpack rev)
    (4,0,9) ->  do
      let result = couchResponse (rspBody r)
      let errorObj (Object x) = fromJust . lookup (T.pack "reason")$ fromObject x
          errorObj x = x
      let (String reason) = errorObj . fromJust $ lookup "error" result
      return $ Left (T.unpack reason)
    otherwise -> error (show r)


updateDoc :: (ToJSON a)
          => String -- ^database
          -> (String,String) -- ^document and revision
          -> a -- ^ new value
          -> CouchMonad (Maybe (String,String))
updateDoc db (doc,rev) val = do
  let (Object obj) = toJSON val
  let doc' = doc
  let obj' = (T.pack "_id",String $ T.pack doc):(T.pack "_rev",String $ T.pack rev):(fromObject obj)
  r <- request (db ++ "/" ++ doc') [] PUT [] (LB.unpack $ encode obj')
  case rspCode r of
    (2,0,1) ->  do
      let result = couchResponse (rspBody r)
      let (String rev) = fromJust $ lookup "rev" result
      return $ Just (doc,T.unpack rev)
    (4,0,9) ->  return Nothing
    otherwise ->
      error $ "updateDoc error.\n" ++ (show r) ++ rspBody r

bulkUpdateDocs :: (ToJSON a)
               => String -- ^database
               -> [a] -- ^ all docs
               -> CouchMonad (Maybe [Either String (String, String)]) -- ^ error or (id,rev)
bulkUpdateDocs db docs = do
  let obj = [(T.pack "docs", docs)]
  r <- request (db ++ "/_bulk_docs") [] POST [] (LB.unpack $ encode obj)
  case rspCode r of
    (2,0,1) ->  do
      let Just results = decode . LB.pack $ rspBody r
      return $ Just $
             map (\result ->
                 case (lookup "id" result,
                       lookup "rev" result) of
                   (Just id, Just rev) -> Right (id, rev)
                   _ -> Left $ fromJust $ lookup "error" result
             ) results
    (4,0,9) ->  return Nothing
    otherwise ->
      error $ "updateDoc error.\n" ++ (show r) ++ rspBody r


-- |Delete a doc by document identifier (revision number not needed).  This
-- operation first retreives the document to get its revision number.  It fails
-- if the document doesn't exist or there is a conflict.
forceDeleteDoc :: String -- ^ database
               -> String -- ^ document identifier
               -> CouchMonad Bool
forceDeleteDoc db doc = do
  r <- getDocPrim db doc
  case r of
    Just (id,rev,_) -> deleteDoc db (id,rev)
    Nothing -> return False

deleteDoc :: String  -- ^database
          -> (String,String) -- ^document and revision
          -> CouchMonad Bool
deleteDoc db (doc,rev) = do
  r <- request (db ++ "/" ++ (doc)) [("rev", rev)]
         DELETE [] ""
  case rspCode r of
    (2,0,0) -> return True
    -- TODO: figure out which error codes are normal (delete conflicts)
    otherwise -> fail $ "deleteDoc failed: " ++ (show r)


newDoc :: (ToJSON a)
       => String -- ^database name
      -> a       -- ^document body
      -> CouchMonad (String,String) -- ^ id and rev of new document
newDoc db doc = do
  obj <- assertObject (toJSON doc)
  r <- request db [] POST [] (LB.unpack $ encode obj)
  case rspCode r of
    (2,0,1) -> do
      let result = couchResponse (rspBody r)
      let (String rev) = fromJust $ lookup "rev" result
      let (String id) = fromJust $ lookup "id" result
      return (T.unpack id, T.unpack rev)
    otherwise -> error (show r)

getDoc :: (FromJSON a)
       => String -- ^database name
       -> String -- ^document name
       -> CouchMonad (Maybe (String,String,a)) -- ^'Nothing' if the
                                                   -- doc does not exist
getDoc dbName docName = do
  r <- request' (dbName ++ "/" ++ docName) GET
  case rspCode r of
    (2,0,0) -> do
      let result = map (\(k,v) -> (T.pack k,v)) $ couchResponse (rspBody r)
      let (String rev) = fromJust $ lookup (T.pack "_rev") result
      let (String id) = fromJust $ lookup (T.pack "_id") result
      case fromJSON (object result) of
        Success val -> return $ Just (T.unpack id, T.unpack rev, val)
        _           -> fail $ "error parsing: " ++ (LB.unpack $ encode $ object result)
    (4,0,4) -> return Nothing -- doc does not exist
    otherwise -> error (show r)

-- |Gets a document as a raw JSON value.  Returns the document id,
-- revision and value as a 'Object'.  These fields are queried lazily,
-- and may fail later if the response from the server is malformed.
getDocPrim :: String -- ^database name
           -> String -- ^document name
           -> CouchMonad (Maybe (String,String,[(String,Value)]))
           -- ^'Nothing' if the document does not exist.
getDocPrim db doc = do
  r <- request' (db ++ "/" ++ doc) GET
  case rspCode r of
    (2,0,0) -> do
      let obj = couchResponse (rspBody r)
      let ~(String rev) = fromJust $ lookup "_rev" obj
      let ~(String id) = fromJust $ lookup "_id" obj
      return $ Just (T.unpack id, T.unpack rev, obj)
    (4,0,4) -> return Nothing -- doc does not exist
    code -> fail $ "getDocPrim: " ++ show code ++ " error"

-- |Gets a document as a Maybe String.  Returns the raw result of what
-- couchdb returns.  Returns Nothing if the doc does not exist.
getDocRaw :: String -> String -> CouchMonad (Maybe String)
getDocRaw db doc = do
  r <- request' (db ++ "/" ++ doc) GET
  case rspCode r of
    (2,0,0) -> do
      return $ Just (rspBody r)
    (4,0,4) -> return Nothing -- doc does not exist
    code -> fail $ "getDocRaw: " ++ show code ++ " error"



getAndUpdateDoc :: (FromJSON a, ToJSON a)
                => String -- ^database
                -> String -- ^document name
                -> (a -> IO a) -- ^update function
                -> CouchMonad (Maybe String) -- ^If the update succeeds,
                                             -- return the revision number
                                             -- of the result.
getAndUpdateDoc db docId fn = do
  r <- getDoc db docId
  case r of
    Just (id,rev,val) -> do
      val' <- liftIO (fn val)
      r <- updateDoc db (id,rev) val'
      case r of
        Just (id,rev) -> return (Just rev)
        Nothing -> return Nothing
    Nothing -> return Nothing


allDocRow :: Value -> Maybe String
allDocRow (Object row) = case lookup (T.pack "key") (fromObject row) of
  Just (String s) -> let key = T.unpack s
                         in case key of
                              '_':_ -> Nothing
                              otherwise -> Just $ T.unpack s
  Just _ -> error $ "key not a string in row " ++ show row
  Nothing -> error $ "no key in a row " ++ show row
allDocRow v = error $ "expected row to be an object, received " ++ show v

getAllDocIds ::String -- ^database name
             -> CouchMonad [String]
getAllDocIds db = do
  response <- request' (db ++ "/_all_docs") GET
  case rspCode response of
    (2,0,0) -> do
      let result = couchResponse (rspBody response)
      let (Array rows) = fromJust $ lookup "rows" result
      return $ mapMaybe allDocRow $ V.toList rows
    otherwise -> error (show response)

--
-- $views
-- Creating and querying views
--

data CouchView = ViewMap String String
               | ViewMapReduce String String String

couchViewToJSON :: CouchView -> (String,Value)
couchViewToJSON (ViewMap name fn) = (name, object fn') where
  fn' = [(T.pack "map", String $ T.pack fn)]
couchViewToJSON (ViewMapReduce name m r) =
  (name, object obj) where
    obj = [(T.pack "map", String $ T.pack m),
           (T.pack "reduce", String $ T.pack r)]

newView :: String -- ^database name
        -> String -- ^view set name
        -> [CouchView] -- ^views
        -> CouchMonad ()
newView dbName viewName views = do
  let content = map ((\(k,v) -> (T.pack k, v)) . couchViewToJSON) views
      body = object
        [((T.pack "language"), String $ T.pack "javascript"),
         ((T.pack "views"), object content)]
      path = "_design/" ++ viewName
  result <- newNamedDoc dbName path body
  case result of
    Right _ -> return ()
    Left err -> do
        let update x = let Object ob = object . map replace $ fromObject x
                       in return ob
            replace (k, Object v) | k == T.pack "views" =
                                (k, object . unite $ fromObject v)
            replace x = x
            unite x = L.nubBy (\(k1, _) (k2, _) -> k1 == k2) $ content ++ x
        res <- getAndUpdateDoc dbName path update
        when (isNothing res) (error "newView: creation of the view failed")

toRow :: FromJSON a => Value -> (String,a)
toRow (Object objVal) = (key,value) where
   obj = fromObject objVal
   key = T.unpack $ case lookup (T.pack "id") obj of
     Just (String s) -> s
     Just v -> error $ "toRow: expected id to be a string, got " ++ show v
     Nothing -> error $ "toRow: row does not have an id field in "
                        ++ show obj
   value = case lookup (T.pack "value") obj of
     Just v -> case fromJSON v of
       Success v' -> v'
       Error s -> error s
     Nothing -> error $ "toRow: row does not have a value in " ++ show obj
toRow val =
  error $ "toRow: expected row to be an object, received " ++ show val


getAllDocs :: (FromJSON a, ToJSON a)
           => String -- ^databse
           -> [(String, Value)] -- ^query parameters
          -- |Returns a list of rows.  Each row is a key, value pair.
          -> CouchMonad [(String, a)]
getAllDocs db args = do
  let args' = map (\(k,v) -> (k,LB.unpack $ encode v)) args
  let url' = concat [db, "/_all_docs"]
  r <- request url' args' GET [] ""
  case rspCode r of
    (2,0,0) -> do
      let result = couchResponse (rspBody r)
      let (Array rows) = fromJust $ lookup "rows" result
      return $ map toRowDoc $ V.toList rows
    otherwise -> error $ "getAllDocs: " ++ show r


toRowDoc :: FromJSON a => Value -> (String,a)
toRowDoc (Object objVal) = (key,value) where
   obj = fromObject objVal
   key = T.unpack $ case lookup (T.pack "id") obj of
     Just (String s) -> s
     Just v -> error $ "toRowDoc: expected id to be a string, got " ++ show v
     Nothing -> error $ "toRowDoc: row does not have an id field in "
                        ++ show obj
   value = case lookup (T.pack "doc") obj of
     Just v -> case fromJSON v of
        Success v' -> v'
        Error s -> error s
     Nothing -> error $ "toRowDoc: row does not have a value in " ++ show obj
toRowDoc val =
  error $ "toRowDoc: expected row to be an object, received " ++ show val


queryView :: (FromJSON a, ToJSON a)
          => String  -- ^database
          -> String  -- ^design
          -> String  -- ^view
          -> [(String, Value)] -- ^query parameters
          -- |Returns a list of rows.  Each row is a key, value pair.
          -> CouchMonad [(String, a)]
queryView db viewSet view args = do
  let args' = map (\(k,v) -> (k, LB.unpack $ encode v)) args
  let url' = concat [db, "/_design/", viewSet, "/_view/", view]
  r <- request url' args' GET [] ""
  case rspCode r of
    (2,0,0) -> do
      let result = couchResponse (rspBody r)
      let (Array rows) = fromJust $ lookup "rows" result
      return $ map toRow $ V.toList rows
    otherwise -> error (show r)

-- |Like 'queryView', but only returns the keys.  Use this for key-only
-- views where the value is completely ignored.
queryViewKeys :: String  -- ^database
            -> String  -- ^design
            -> String  -- ^view
            -> [(String, Value)] -- ^query parameters
            -> CouchMonad [String]
queryViewKeys db viewSet view args = do
  let args' = map (\(k,v) -> (k, LB.unpack $ encode v)) args
  let url' = concat [db, "/_design/", viewSet, "/_view/", view]
  r <- request url' args' GET [] ""
  case rspCode r of
    (2,0,0) -> do
      let result = couchResponse (rspBody r)
      case lookup "rows" result of
        Just (Array rows) -> liftIO $ mapM rowKey $ V.toList rows
        otherwise -> fail $ "queryView: expected rows"
    otherwise -> error (show r)

rowKey :: Value -> IO String
rowKey (Object obj) = do
  let assoc = fromObject obj
  case lookup (T.pack "id") assoc of
    Just (String s) -> return (T.unpack s)
    v -> fail "expected id"
rowKey v = fail "expected id"
