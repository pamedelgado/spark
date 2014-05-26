/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.Collection
import org.apache.ivy.core.resolve.IvyNode
import org.apache.ivy.plugins.conflict.AbstractConflictManager
import scala.collection.JavaConversions._

class NearestConflictManager extends AbstractConflictManager {
  setName("nearest")

  /**
   * It first looks in the current subprojects deps and then its parents and then so on.
   * In case it does not find any thing we look for latest version.
   */
  def findNearestVersion(parent: IvyNode, node: IvyNode): Option[IvyNode] = {
    parent.getRealNode.hasProblem
    None
  }


  def resolveConflicts(parent: IvyNode, conflicts: Collection[_]): Collection[_] = {
    // These are added by SBT mainly scala library jars and should take highest precedence.
    val buildDeps = parent.getDependencies("*", "scala-tool", "*").toArray.distinct
    val compileDeps = parent.getDependencies("*", "compile", "*").toArray.distinct
    val testDeps = parent.getDependencies("*","test","*").toArray.distinct
    // We ignore plugin jars for conflict resolution.
    // val pDeps = parent.getDependencies("*","plugin","*").toArray.distinct
    if(conflicts.size() == 1)
      // No need of conflict resolution.
      conflicts
    else {

      println("conflict:"+conflicts.mkString("<>"))
      println("compileDeps:"+compileDeps.mkString("--"))
      println("testDeps:"+testDeps.mkString("--"))
      println("buildDeps"+buildDeps.mkString("--"))
      println("root Project:")
      conflicts
    }
  }

}

