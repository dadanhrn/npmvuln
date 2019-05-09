package npmvuln.jobs

import java.lang

import org.apache.hadoop.io.{ArrayWritable, LongWritable, ObjectWritable}
import org.apache.giraph.graph.{BasicComputation, Vertex}
import npmvuln.props._

//object Giraph_PackageVerticesBuilder
//  extends BasicComputation[LongWritable, ObjectWritable, ObjectWritable, ArrayWritable] {
//
//  override def compute(vertex: Vertex[LongWritable, ObjectWritable, ObjectWritable],
//                       messages: lang.Iterable[ArrayWritable]): Unit = {
//    messages.iterator()
//  }
//
//}
