package main


import (
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"strings"
	"fmt"
	"database/sql"
)

const (
	destinationUniquifier = ": "
	globalDbDriver = "sqlite3"
	globalDbConnString = "file::memory:?mode=memory&cache=shared"
)

func execute(js *aql.JobScript, options []aql.Option, logger engine.Logger, compileOnly bool) error {
	dag := engine.NewCoordinator(logger)
	err := js.ResolveExternalContent()
	if err != nil {
		return fmt.Errorf("error resolving external content: %v", err)
	}
	err = js.EvaluateParametrizedContent(options)
	if err != nil {
		return fmt.Errorf("error evaluating parametrized content: %v", err)
	}
	connMap, err := connectionMap(js)
	if err != nil {
		return fmt.Errorf("error parsing connections: %v", err)
	}

	err = globalInit(js)

	if err != nil {
		return err
	}

	err = sources(js, dag, connMap)

	if err != nil {
		return err
	}

	err = destinations(js, dag, connMap)

	if err != nil {
		return err
	}

	err = constraints(js, dag, connMap)

	if err != nil {
		return err
	}

	err = dag.Compile()

	if err != nil {
		return err
	}

	if compileOnly {
		return nil
	}
	return dag.Execute()
}

func ExecuteString(script string, options []aql.Option, logger engine.Logger) error {
	js, err := aql.ParseString(script)
	if err != nil {
		return err
	}
	return execute(js, options, logger, false)
}

func ExecuteFile(filename string, options []aql.Option, logger engine.Logger) error {
	js, err := aql.ParseFile(filename)
	if err != nil {
		return err
	}
	return execute(js, options, logger, false)
}

func ValidateString(script string, options []aql.Option, logger engine.Logger) error {
	js, err := aql.ParseString(script)
	if err != nil {
		return err
	}
	return execute(js, options, logger, true)
}

func ValidateFile(filename string, options []aql.Option, logger engine.Logger) error {
	js, err := aql.ParseFile(filename)
	if err != nil {
		return err
	}
	return execute(js, options, logger, true)
}

//globalInit initializes the GLOBAL db based on user-defined queries
//Any valid SQL can be used to initialize the database.
//Currently, the GLOBAL database must live in-memory. In future releases
//the SET [OPTION_NAME] [OPTION_VALUE] syntax will be available to configure this.
func globalInit(js *aql.JobScript) error {
	db, err := sql.Open(globalDbDriver, globalDbConnString)
	if err != nil {
		return err
	}

	for _, block := range js.Globals {
		_, err := db.Exec(block.Content)
		if err != nil {
			return fmt.Errorf("error initializing GLOBAL with block %s: %v", block.Name, err)
		}
	}

	return nil
}

//constraints applies AFTER constraints.
//As of current release:
//  - Limited to QUERY blocks
func constraints(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection) error {
	for _, query := range js.Queries {
		for _, before := range query.Dependencies {
			err := dag.AddConstraint(strings.ToLower(before), strings.ToLower(query.Name))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//sources makes engine.Source s out of JobScript sources.
//As of current release:
//	- Limited to SQL sources (Excel sources require scripts or built-ins to process data which won't come until vNext)
//	- Queries limited to single source (this will probably remain a limitation for the foreseeable future)
func sources(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection) error {
	for _, query := range js.Queries {
		if len(query.Sources) != 1 {
			return fmt.Errorf("queries must have exactly one source but %s has %v", query.Name, len(query.Sources))
		}
		if query.Sources[0].Global {
			g := engine.SQLSource{
				Driver: globalDbDriver ,
				ConnectionString: globalDbConnString,
				Query: query.Content,
			}
			alias := alias(query.Sources[0], nil)
			g.SetName(alias)
			dag.AddSource(strings.ToLower(query.Name), alias, &g)
			continue
		}
		if query.Sources[0].Database == nil {
			return fmt.Errorf("at present only GLOBAL and CONNECTION sources are supported for query %s", query.Name)
		}
		if connMap[strings.ToLower(*query.Sources[0].Database)] == nil {
			return fmt.Errorf("could not find connection %s for query %s", *query.Sources[0].Database, query.Name)
		}
		conn := connMap[strings.ToLower(*query.Sources[0].Database)]
		s := engine.SQLSource{
			Driver: conn.Driver,
			ConnectionString: conn.ConnectionString,
			Query: query.Content,
		}
		alias := alias(query.Sources[0], conn)
		s.SetName(alias)
		dag.AddSource(strings.ToLower(query.Name), alias, &s)
	}
	return nil
}

func alias(ss aql.SourceSink, conn *aql.Connection) string {
	if ss.Alias != nil {
		return *ss.Alias
	}
	if ss.Global {
		return "GLOBAL"
	}
	if conn == nil {
		panic("alias panic: should be unreachable")
	}
	return conn.Name

}

//TODO: refactor all this option parsing nonsense
func sqlDest(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, query aql.Query, conn aql.Connection, dest aql.SourceSink) error {
	driver := conn.Driver
	connString := conn.ConnectionString

	tableOpt, ok := aql.FindOverridableOption("TABLE", conn.Name, query.Options, conn.Options)

	if !ok {
		return fmt.Errorf("expected TABLE option for connection %s in the connection definition or the query %s options", conn.Name, query.Name)
	}

	table, ok := tableOpt.String()

	if !ok {
		return fmt.Errorf("expected TABLE option to be a STRING for connection %s and query %s", conn.Name, query.Name)
	}
	alias := alias(dest, &conn)

	//Uniquify destination name
	dag.AddDestination(strings.ToLower(query.Name + destinationUniquifier + conn.Name), alias, &engine.SQLDestination{
		Name: query.Name + destinationUniquifier + conn.Name,
		Driver: driver,
		ConnectionString: connString,
		Table: table,
		Alias: alias,
	})

	dag.Connect(strings.ToLower(query.Name), strings.ToLower(query.Name + destinationUniquifier + conn.Name))

	return nil

}

//TODO: refactor all this option parsing nonsense
func globalDest(js *aql.JobScript, dag engine.Coordinator, query aql.Query, dest aql.SourceSink) error {
	driver := globalDbDriver
	connString := globalDbConnString

	tableOpt, ok := aql.FindOption(query.Options, "TABLE")

	if !ok {
		return fmt.Errorf("expected TABLE option for GLOBAL connection in the query %s options", query.Name)
	}

	table, ok := tableOpt.String()

	if !ok {
		return fmt.Errorf("expected TABLE option to be a STRING for GLOBAL connection and query %s", query.Name)
	}

	alias := alias(dest, nil)

	//Uniquify destination name
	dag.AddDestination(strings.ToLower(query.Name + destinationUniquifier + "GLOBAL"), alias, &engine.SQLDestination{
		Name: query.Name + destinationUniquifier + "GLOBAL",
		Driver: driver,
		ConnectionString: connString,
		Table: table,
		Alias: alias,
	})

	dag.Connect(strings.ToLower(query.Name), strings.ToLower(query.Name + destinationUniquifier + "GLOBAL"))

	return nil

}

//TODO: refactor all this option parsing nonsense
func excelDest(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, query aql.Query, conn aql.Connection, dest aql.SourceSink) error {
	//register Excel destination

	fileOpt, ok := aql.FindOption(conn.Options, "FILE")
	if !ok {
		return fmt.Errorf("connection %s should specify FILE option", conn.Name)
	}

	file, ok := fileOpt.String()

	if !ok {
		return fmt.Errorf("expected FILE option to be a STRING for connection %s and query %s", conn.Name, query.Name)
	}

	sheetOpt, ok := aql.FindOverridableOption("SHEET", conn.Name, query.Options, conn.Options)

	if !ok {
		return fmt.Errorf("expected SHEET option for connection %s in the connection definition or the query %s options", conn.Name, query.Name)
	}

	sheet, ok := sheetOpt.String()

	if !ok {
		return fmt.Errorf("expected SHEET option to be a STRING for connection %s and query %s", conn.Name, query.Name)
	}

	templateOpt, ok := aql.FindOverridableOption("TEMPLATE", conn.Name, query.Options, conn.Options)

	var template string
	if ok {
		var ok2 bool
		template, ok2 = templateOpt.String()
		if !ok2 {
			return fmt.Errorf("expected TEMPLATE option to be a STRING for connection %s and query %s", conn.Name, query.Name)
		}

	}

	overwriteOpt, ok := aql.FindOverridableOption("OVERWRITE", conn.Name, query.Options, conn.Options)

	var overwrite bool
	if ok {
		overwrite = overwriteOpt.Truthy()
	}


	rangOpt, ok := aql.FindOverridableOption("RANGE", conn.Name, query.Options, conn.Options)

	if !ok {
		return fmt.Errorf("expected RANGE option for connection %s in the connection definition or the query %s options", conn.Name, query.Name)
	}

	rang, ok := rangOpt.String()

	if !ok {
		return fmt.Errorf("expected RANGE option to be a STRING for connection %s and query %s", conn.Name, query.Name)
	}

	x1, x2, y1, y2, err := aql.ParseExcelRange(rang)

	if err != nil {
		return err
	}

	var (
		xx2 engine.ExcelRangePoint
		yy2 engine.ExcelRangePoint
	)

	if x2 == nil {
		xx2.N = true
	} else {
		xx2.P = *x2
	}

	if y2 == nil {
		yy2.N = true
	} else {
		yy2.P = *y2
	}

	var (
		transpose bool
		columns []string
	)


	trs, ok := aql.FindOverridableOption("TRANSPOSE", conn.Name, query.Options, conn.Options)

	if ok {
		transpose = trs.Truthy()
	}

	colsOpt, ok := aql.FindOverridableOption("COLUMNS", conn.Name, query.Options, conn.Options)



	if ok {
		cols, ok2 := colsOpt.String()
		if !ok2 {
			return fmt.Errorf("expected COLUMNS option to be a STRING for connection %s and query %s", conn.Name, query.Name)
		}
		columns = strings.Split(cols, ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	alias := alias(dest, &conn)
	//Make destination name unique
	dag.AddDestination(strings.ToLower(query.Name + destinationUniquifier + conn.Name), alias, &engine.ExcelDestination{
		Name: query.Name + destinationUniquifier + conn.Name,
		Filename: file,
		Sheet: sheet,
		Range: engine.ExcelRange{
			X1: x1,
			Y1: y1,
			X2: xx2,
			Y2: yy2,
		},
		Transpose: transpose,
		Cols: columns,
		Overwrite: overwrite,
		Template: template,
		Alias: alias,
	})

	dag.Connect(strings.ToLower(query.Name), strings.ToLower(query.Name + destinationUniquifier + conn.Name))

	return nil

}

//destinations makes engine.Destination s out of JobScript destinations and connects sources to them.
//As of current release:
//  - Limited to SQL or Excel destinations
//  - Multiple destinations supported for queries. The table for multiple destinations needs to be specified as TABLE_{DEST_NAME} = '{TABLE_NAME}'
//  - GLOBAL, SCRIPT and BLOCK destinations not supported
func destinations(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection) error{
	for _, query := range js.Queries {
		for _, dest := range query.Destinations {
			if dest.Script != nil || dest.Block != nil {
				return fmt.Errorf("only GLOBAL, SQL and Excel destinations are currently supported for query %s", query.Name)
			}
			if dest.Global {
				if err := globalDest(js, dag, query, dest); err != nil {
					return err
				}
				continue
			}
			if dest.Database != nil && connMap[strings.ToLower(*dest.Database)] == nil {
				return fmt.Errorf("destination %s not found for query %s", *dest.Database, query.Name)
			}
			conn := *connMap[strings.ToLower(*dest.Database)]
			var err error
			if strings.ToUpper(conn.Driver) == "EXCEL" {
				err = excelDest(js, dag, connMap, query, conn, dest)
			} else {
				err = sqlDest(js, dag, connMap, query, conn, dest)
			}
			if err != nil {
				return err
			}

		}
	}
	return nil
}

func connectionMap(js *aql.JobScript) (map[string]*aql.Connection, error) {
	conns, err := js.ParseConnections()
	if err != nil {
		return nil, err
	}
	ret := make(map[string]*aql.Connection)
	for k, conn := range conns {
		ret[strings.ToLower(conn.Name)] = &conns[k]
	}
	return ret, nil
}

