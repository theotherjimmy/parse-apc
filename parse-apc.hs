#!/usr/bin/runhaskell

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE BangPatterns#-}

import Control.Monad
import Control.Applicative
import Data.Attoparsec.ByteString.Lazy as A
import Data.Attoparsec.Binary
import Data.Bits
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BSS
import qualified Data.Map.Strict as M
import Data.Either
import Data.Foldable
import Data.List (intercalate)
import Data.Maybe
import Data.Word
import Options.Applicative
import qualified Options.Applicative as Opt
import Prelude hiding (take)
import Text.XML.HXT.Core
import Text.XML.HXT.XPath.Arrows

data Arguments = Arguments { inputfile :: String
                           , capture :: String
                           }

argParser :: Opt.Parser Arguments
argParser = Arguments <$> argument str (metavar "00000000")
  <*> argument str (metavar "capture.xml")

parseAPC :: BSL.ByteString -> Maybe [Either String Frame]
parseAPC = maybeResult . parse fileParser

fileParser :: A.Parser [Either String Frame]
fileParser = manyTill apcFrame endOfInput

apcFrame :: A.Parser (Either String Frame)
apcFrame =  parseFrame <$> (anyWord32le >>= take . fromIntegral)

parseFrame :: BSS.ByteString -> Either String Frame
parseFrame =  parseOnly frameParser
  where frameParser = packed32 <* packed_ >>= frameFromNum

data Frame = Counter { counterTimestamp :: Word64
                     , counterKey :: Word32
                     , counterValue :: Word64}
           | BlockCounter [(Word64, (M.Map Word32 Word64))]
           | DontCare
             deriving (Show)

data SchedTraceFrame = Switch { switchTid :: Word32
                              , switchState :: Word32}
                     | ThreadExit { exitTid :: Word32}
                     deriving (Show)

frameFromNum :: (Num a, Eq a) => a -> A.Parser Frame
frameFromNum 1 =  DontCare
  <$ stringN "1\n2\r\n3\r4\n\r5" <* packed_ <* packed_
  <* manyTill (takeString_ *> takeString_) (word8 0 *> endOfInput)
frameFromNum 4 = Counter <$> packed64 <* packed_ <*> packed32 <*> packed64
frameFromNum 5 = BlockCounter . blockToMaps
  <$> manyTill ((,) <$> packed32 <*> packed64) endOfInput
frameFromNum _ = pure DontCare

blockToMaps :: (Num k, Ord k, Foldable t) => t (k, a) -> [(a, M.Map k a)]
blockToMaps m = foldl doMap [] m
  where doMap [] (0, v) = [(v, M.empty)]
        doMap x (0, v) = (v,M.empty) : x
        doMap ((ts,x):xs) (key, v) = (ts,M.insert key v x) : xs
        doMap x _ = x

stringN :: BSS.ByteString -> A.Parser BSS.ByteString
stringN s = word8 (fromIntegral $ BSS.length s) *> string s

takeString_ :: A.Parser ()
takeString_ =  (packed32 >>= void . take . fromIntegral)

packed32 :: A.Parser Word32
packed32 = packed
packed64 :: A.Parser Word64
packed64 = packed

packed :: (Num a, Bits a) => A.Parser a
packed = packedToWord <$> scan False highBitSet
  where highBitSet False char | (char >= 128) = Just False
                              | otherwise = Just True
        highBitSet True _ = Nothing
        packedToWord = BSS.foldr eachByte 0
        eachByte byte acc = (shift acc 7) .|. fromIntegral (0x7F .&. byte)

packed_ ::  A.Parser ()
packed_ = void $ scan False highBitSet
  where highBitSet False char | (char >= 128) = Just False
                              | otherwise = Just True
        highBitSet True _ = Nothing

prettyPrint :: Foldable t => M.Map Word32 b -> t Frame -> IO ()
prettyPrint keys l = foldlM helper (0, M.map (const 0) keys) l >>= \(ts, out) -> printRow ts out
  where helper (ts, prev) a@(Counter nts key val) | nts /= ts = printRow ts prev >> helper (nts, prev) a
                                                  | otherwise = pure (nts, M.insert key val prev)
        helper b (BlockCounter x) = foldlM blockhelper b x
          where blockhelper (ts, prev) (nts, m) | (nts /= ts) = printRow ts prev >> blockhelper (nts, prev) (nts, m)
                                                | otherwise = pure (nts, M.union m prev)
        helper b _= pure b

printRow :: (Eq a, Num a, Show a, Show b) => a -> M.Map k b -> IO ()
printRow 0 _ = pure ()
printRow ts m = putStrLn $ mappend (show ts <> ", ") $ intercalate ", " $ map show $ M.elems m

getKeys :: IOSLA (XIOState ()) XmlTree (Word32, String)
getKeys = isElem >>> hasName "counter" >>> proc c -> do
  key <- getAttrValue "key" -< c
  name <- getAttrValue "type" -< c
  returnA -< (read key :: Word32, name)

main :: IO ()
main = execParser opts >>= \args -> do
  keys <- M.fromList <$> (runX $ readDocument [] (capture args)
                          >>> getXPathTrees "/captured/counters/counter"
                          >>> getKeys)
  putStrLn $ mappend ("Timestamp, ") $ intercalate ", " $  M.elems keys
  BSL.readFile (inputfile args) >>=  prettyPrint keys . fromMaybe [] . fmap rights . parseAPC
  where opts = info (helper <*> argParser) (fullDesc)
