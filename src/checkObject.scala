
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
object checkObject {

  def main(args: Array[String]) {
    val inputString = "9189070480200000161C,918907048040000008FF,91890704801000012889,9189070480100000DD4D,91890704801000004FEB,91890704801000008772,91890704801000019A2A,34161FA820328EE8022EA6A0"
    val input = inputString.split(",")
    val partition = 3

    var i = 0;

    while (i < input.length) {
      var j = 0;
      while (j < partition && (i + j) < input.length) {
        print(input(i + j))
        if (j != partition - 1 && (i + j) != input.length - 1) {
          print(",")
        }
          j = j + 1;
      }
      println
      i = i + partition
    }

  }

  def printGroup(input: Array[String], i: Integer, partition: Integer) = {
    var j = 0;
    while (j < partition && (i + j) < input.length) {
      print(input(i + j) + ",")
      j = j + 1;
    }
  }
  def count(input: Array[String], i: Integer): Unit = {
    if (i + 1 < input.length) {
      print(input(i) + "," + input(i + 1))
      println
      count(input, i + 2)
    }

  }

}
