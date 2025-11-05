package sql

type Middleware func(Queryable) Queryable

func New(q Queryable, mws ...Middleware) Queryable {
	for _, mw := range mws {
		q = mw(q)
	}
	return q
}
