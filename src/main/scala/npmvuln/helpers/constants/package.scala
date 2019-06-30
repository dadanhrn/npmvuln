package npmvuln.helpers

import java.time.Instant
import java.util.Date

package object constants {
  // Censor date: 2017-11-10 00:00:00 UTC
  val CENSOR_DATE: Instant = new Date(117,10,11,-17,0,0).toInstant
}
