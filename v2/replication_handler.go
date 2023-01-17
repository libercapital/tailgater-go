package tailgater

import "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/v2/internal/pgoutput"

func handler(set *pgoutput.RelationSet, publish func(relation uint32, row []pgoutput.Tuple) error) func(m pgoutput.Message) error {
	return func(m pgoutput.Message) error {
		switch v := m.(type) {
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
			return publish(v.RelationID, v.Row)
		}
		return nil
	}
}
