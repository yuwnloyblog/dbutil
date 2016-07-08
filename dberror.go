package dbutil
import(
	"fmt"
	"time"
)
type DbUtilError struct{
	What string
}
func (self *DbUtilError) Error() string {
	return fmt.Sprintf("at %v, %s",
		time.Now(), self.What)
}