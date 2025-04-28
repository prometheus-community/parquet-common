package util

// Intersects returns if [a, b] and [c, d] intersect.
func Intersects(a, b, c, d int64) bool {
	return !(c > b || a > d)
}
