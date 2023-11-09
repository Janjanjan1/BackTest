"""
Author: Ken Lambert, Liz Matthews
This is an example code for one approach to this problem.
Use of this code is for learning purposes only- do not upload it anywhere.
"""

import random
import math


def main():
   count = 0
   upperBound = 100
   lowerBound = 0
   maxCount = int(math.log(upperBound - lowerBound, 2)) + 1
   
   # Prompt the user and wait for enter to let them think of a number
   print("Think of a number between", lowerBound, "and", upperBound, "and press enter to start.")
   input()
   
   while count < maxCount:
      count += 1
       
      # Guess at the exact middle of the range we know the number is within
      computerNumber = (lowerBound + upperBound) // 2
      print("My guess is", computerNumber, "!")
      
      hint = input("How did I do? [too small, too large, correct]: ")
       
      if hint == "too small":
         # We guessed too small, lower bound is updated
         lowerBound = computerNumber + 1x
         
      elif hint == "too large":
         # We guessed too big, upper bound is updated
         upperBound = computerNumber - 1
         
      else:
         print("I got it in", count, "tries!")
         break
   
   if count == maxCount:
      print("I didn't get it :(")
   

if __name__ == "__main__":
   main()