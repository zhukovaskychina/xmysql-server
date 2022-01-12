package store

type bpt_iterator func() (pageNo uint32, idxRecord int, err error, bi bpt_iterator)
