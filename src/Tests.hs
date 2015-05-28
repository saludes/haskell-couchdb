{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad.Trans (liftIO)
import Control.Exception (SomeException, catch, finally)
import Control.Applicative ((<$>), (<*>))
import Test.HUnit
import Database.CouchDB
import Database.CouchDB.JSON
import Data.Aeson
import System.Exit
import qualified Data.Text as T
import Control.Monad

-- ----------------------------------------------------------------------------
-- Helper functions
--

runCouchDB2 :: CouchMonad a -> IO a
runCouchDB2 = runCouchDB "192.168.99.100" 32785

assertDBEqual :: (Eq a, Show a) => String -> a -> CouchMonad a -> Assertion
assertDBEqual msg v m = do
  v' <- runCouchDB2 m
  assertEqual msg v' v

instance Assertable (Either String a) where
  assert (Left s) = assertFailure s
  assert (Right _) = return ()

assertRight :: (Either String a) -> IO a
assertRight (Left s) = assertFailure s >> fail "assertion failed"
assertRight (Right a) = return a

instance Assertable (Maybe a) where
  assert Nothing = assertFailure "expected (Just ...), got Nothing"
  assert (Just a) = return ()

assertJust :: Maybe a -> IO a
assertJust (Just v) = return v
assertJust Nothing = do
  assertFailure "expected (Just ...), got Nothing"
  fail "assertion failed"

testWithDB :: String -> (DB -> CouchMonad Bool) -> Test
testWithDB testDescription testCase =
  TestLabel testDescription $ TestCase $ do
    let action = runCouchDB2 $ do
          createDB "haskellcouchdbtest"
          result <- testCase (db "haskellcouchdbtest")
          liftIO $ assertBool testDescription result
    let teardown = runCouchDB2 (dropDB "haskellcouchdbtest")
    let failure :: SomeException -> Assertion
        failure _ = assertFailure (testDescription ++ "; exception signalled")
    action `catch` failure `finally` teardown

main = do
  putStrLn "Running CouchDB test suite..."
  results <- runTestTT allTests
  when (errors results > 0 || failures results > 0)
    exitFailure
  putStrLn "Testing complete."
  return ()

-- -----------------------------------------------------------------------------
-- Data definitions for testing
--

data Age = Age
  { ageName :: String
  , ageValue :: Int
  } deriving (Eq,Show)

instance ToJSON Age where
  toJSON (Age name val) =
    object ["name" .= name, "age" .=  val]

instance FromJSON Age where
  parseJSON (Object v) = Age <$>
    v .:  "name" <*>
    v .:  "age"
  parseJSON _          = mzero

-- ----------------------------------------------------------------------------
-- Test cases
--


testCreate = TestCase $ assertDBEqual "create/drop database" True $ do
  dropDB "haskellcouchdbtest"
  createDB "haskellcouchdbtest"
  dropDB "haskellcouchdbtest" -- returns True since the database exists.

people = [ Age "Arjun" 18, Age "Alex" 17 ]

testNamedDocs = testWithDB "add named documents" $ \mydb -> do
  newNamedDoc mydb (doc "arjun") (people !! 0)
  newNamedDoc mydb (doc "alex") (people !! 1)
  Just (_,_,v1) <- getDoc mydb (doc "arjun")
  Just (_,_,v2) <- getDoc mydb (doc "alex")
  return $ (v1 == people !! 0) && (v2 == people !! 1)

testUTF8 = testWithDB "test UTF8 characters" $ \db -> do
  newNamedDoc db (doc "d0") (Age "äöüß" 900)
  Just (_, _, d) <- getDoc db (doc "d0")
  return (ageName d == "äöüß")


allTests = TestList [ testCreate, testNamedDocs, testUTF8 ]
