-- |Convenient functions for parsing JSON responses.  Use these
-- functions to write the 'readJSON' method of the 'JSON' class.
module Database.CouchDB.JSON
  ( jsonString
  , jsonInt
  , jsonObject
  , jsonField
  , jsonBool
  , jsonIsTrue
  ) where

import Data.Aeson
import qualified Data.HashMap.Strict as M
import qualified Data.Scientific as S
import qualified Data.Text as T
import Data.Ratio (numerator,denominator)

jsonString :: Value -> Result T.Text
jsonString (String s) = return s
jsonString _ = fail "expected a string"

jsonInt :: (Integral n) => Value -> Result n
jsonInt (Number n) =
  case S.floatingOrInteger n of
    Right i -> return $ fromIntegral i
    Left _  -> fail "expected an integer"

jsonObject :: Value -> Result [(String,Value)]
jsonObject (Object obj) = return $ map (\(k,v) -> (T.unpack k,v)) $ M.toList obj
jsonObject v = fail $ "expected an object, got " ++ (show v)

jsonBool :: Value -> Result Bool
jsonBool (Bool b) = return b
jsonBool v = fail $ "expected a boolean value, got " ++ show v

-- |Extract a field as a value of type 'a'.  If the field does not
-- exist or cannot be parsed as type 'a', fail.
jsonField :: FromJSON a => String -> [(String,Value)] -> Result a
jsonField field obj =
  case lookup field obj of
    Just v -> fromJSON v
    Nothing -> fail $ "could not find the field " ++ field

-- |'True' when the field is defined and is true.  Otherwise, 'False'.
jsonIsTrue :: String -> [(String,Value)] -> Result Bool
jsonIsTrue field obj = case lookup field obj of
  Just (Bool True) -> return True
  otherwise -> return False
