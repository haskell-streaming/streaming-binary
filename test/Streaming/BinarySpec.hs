{-# LANGUAGE TypeApplications #-}

module Streaming.BinarySpec where

import Control.Monad (replicateM_, void)
import Data.Binary (put)
import Data.Binary.Put (runPut)
import Data.Function ((&))
import qualified Data.ByteString.Streaming as Q
import Streaming.Binary
import qualified Streaming.Prelude as S
import Test.Hspec

spec :: Spec
spec = do
    let input n = Q.fromLazy $ runPut $ replicateM_ n $ put (42 :: Int)
    describe "decoded" $ do
      it "succeeds on empty inputs" $ do
        output <- void (decoded @Int (input 0)) & S.toList_
        output `shouldBe` []
      it "decodes single integers" $ do
        output <- void (decoded @Int (input 1)) & S.toList_
        output `shouldBe` [42]
      it "decodes multiple integers" $ do
        output <- void (decoded @Int (input 10)) & S.toList_
        output `shouldBe` (replicate 10 42)
      it "decodes multiple integers even when the input is incomplete" $ do
        n <- fromIntegral <$> Q.length_ (input 10)
        let input' = Q.take (n - 1) (input 10)
        output <- void (decoded @Int input') & S.toList_
        output `shouldBe` (replicate 9 42)
      it "leaves the right amount of leftover on incomplete input" $ do
        n <- Q.length_ (input 10)
        let input' = Q.take (fromIntegral (n - 1)) (input 10)
        (leftover, _, _) <- S.effects (decoded @Int input')
        Q.length_ leftover `shouldReturn` (n `div` 10) - 1
