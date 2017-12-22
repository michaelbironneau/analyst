package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"github.com/michaelbironneau/analyst/plugins"
	builtins "github.com/michaelbironneau/analyst/transforms"
	"strings"
)

const (
	sourceUniquifier      = ": "
	destinationUniquifier = ": "
	globalDbDriver        = "sqlite3"
	globalDbConnString    = "file::memory:?mode=memory&cache=shared"
	sqlSelectAll          = "SELECT * FROM %s"
)

func execute(js *aql.JobScript, options []aql.Option, logger engine.Logger, compileOnly bool) error {
	dag := engine.NewCoordinator(logger)
	params := engine.NewParameterTable()
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

	err = declarations(js, params)

	if err != nil {
		return err
	}

	err = globalInit(js)

	if err != nil {
		return err
	}

	err = sources(js, dag, connMap, params)

	if err != nil {
		return err
	}

	err = transforms(js, dag, connMap)

	if err != nil {
		return err
	}

	err = destinations(js, dag, connMap, params)

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

func declarations(js *aql.JobScript, p *engine.ParameterTable) error {
	for _, declaration := range js.Declarations {
		if err := p.Declare(declaration.Name); err != nil {
			return err
		}
	}
	return nil
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
func constraints(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection) error {
	for _, query := range js.Queries {
		for _, before := range query.Dependencies {
			err := dag.AddConstraint(strings.ToLower(before), strings.ToLower(query.Name))
			if err != nil {
				return err
			}
		}
	}
	for _, transform := range js.Transforms {
		for _, before := range transform.Dependencies {
			err := dag.AddConstraint(strings.ToLower(before), strings.ToLower(transform.Name))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//scripts makes engine.Transforms out of JobScript scripts.
//Script sources/destinations are not yet supported. We can have:
//  [NOT YET IMPLEMENTED] script source    -> GLOBAL
//  [NOT YET IMPLEMENTED] script source    -> script transform
//  [NOT YET IMPLEMENTED] script source    -> script destination
//  SQL source       -> script transform
//  GLOBAL           -> script transform
//  script transform -> script transform
//  [NOT YET IMPLEMENTED] script transform -> script destination
//  script transform -> GLOBAL destination
//  script transform -> SQL destination
func transforms(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection) error {
	for _, transform := range js.Transforms {

		var (
			plugin engine.SequenceableTransform
			err    error
		)

		if !transform.Plugin {
			plugin, err = builtins.Parse(transform.Content)
			if err != nil {
				return err
			}
			err = dag.AddTransform(strings.ToLower(transform.Name), strings.ToLower(transform.Name), plugin)
			plugin.SetName(strings.ToLower(transform.Name))
		} else {
			//Create the plugin
			plugin, err = addPlugin(js, dag, transform)
		}

		if err != nil {
			return err
		}

		var sourceSequence []string

		for _, source := range transform.Sources {
			//If the source is a connection rather than a query, it is either:
			//  - Excel spreadsheet with details in connection
			//  - SQL database with SELECT * FROM Table query
			//  - GLOBAL with SELECT * FROM Table query
			var (
				sourceTable     string
				connectionAlias string
			)
			if source.Global || source.Database != nil {
				var ok bool
				connectionAlias = alias(*source, nil)
				tableOpt, ok := aql.FindOverridableOption("TABLE", connectionAlias, transform.Options)

				if !ok {
					return fmt.Errorf("expected TABLE option for %s in the TRANSFORM %s options", connectionAlias, transform.Name)
				}

				sourceTable, ok = tableOpt.String()

				if !ok {
					return fmt.Errorf("expected TABLE option to be a STRING for %s source in TRANSFORM %s", connectionAlias, transform.Name)
				}
			}

			if source.Global {
				g := engine.SQLSource{
					Driver:           globalDbDriver,
					ConnectionString: globalDbConnString,
					Query:            fmt.Sprintf(sqlSelectAll, sourceTable),
				}
				g.SetName(connectionAlias)
				if err := dag.AddSource(strings.ToLower(transform.Name)+sourceUniquifier+connectionAlias, connectionAlias, &g); err != nil {
					return err
				}

				if err := dag.Connect(strings.ToLower(transform.Name)+sourceUniquifier+connectionAlias, strings.ToLower(transform.Name)); err != nil {
					return err
				}
				sourceSequence = append(sourceSequence, connectionAlias)
				continue
			}

			if source.Database != nil {
				if connMap[strings.ToLower(*source.Database)] == nil {
					return fmt.Errorf("could not find connection %s for TRANSFORM %s", *source.Database, transform.Name)
				}
				conn := connMap[strings.ToLower(*source.Database)]

				if strings.ToLower(conn.Driver) == "excel" {
					if err := excelSource(js, dag, connMap, transform, *conn, *source); err != nil {
						return err
					}

					if err := dag.Connect(strings.ToLower(transform.Name)+sourceUniquifier+connectionAlias, strings.ToLower(transform.Name)); err != nil {
						return err
					}

					sourceSequence = append(sourceSequence, connectionAlias)
					continue
				}

				s := engine.SQLSource{
					Driver:           conn.Driver,
					ConnectionString: conn.ConnectionString,
					Query:            fmt.Sprintf(sqlSelectAll, sourceTable),
				}
				s.SetName(connectionAlias)
				if err := dag.AddSource(strings.ToLower(transform.Name)+sourceUniquifier+connectionAlias, connectionAlias, &s); err != nil {
					return err
				}

				if err := dag.Connect(strings.ToLower(transform.Name)+sourceUniquifier+connectionAlias, strings.ToLower(transform.Name)); err != nil {
					return err
				}
				sourceSequence = append(sourceSequence, connectionAlias)
				continue
			}

			//This is a fallthrough in case the source is neither CONNECTION or GLOBAL
			//It must therefore be another TRANSFORM. We don't need to add it yet.
			if source.Block != nil {
				if source.Alias != nil {
					sourceSequence = append(sourceSequence, *source.Alias)
				} else {
					sourceSequence = append(sourceSequence, *source.Block)
				}

				//query is already created, so connect it
				if err := dag.Connect(strings.ToLower(*source.Block), strings.ToLower(transform.Name)); err != nil {
					return err
				}

			}

		}

		//Sequence sources
		if err := sequenceSources(plugin, &transform, sourceSequence); err != nil {
			return err
		}

	}
	return nil
}

func sequenceSources(transform engine.SequenceableTransform, block aql.Block, sourceSequence []string) error {
	//Sequence sources, if required
	var sequence bool

	seq, ok := aql.FindOption(block.GetOptions(), "MULTISOURCE_ORDER")
	if ok {
		seqStr, ok2 := seq.String()

		if !ok2 {
			return fmt.Errorf("expected MULTISOURCE_ORDER option to be a string in transform %s", block.GetName())
		}
		switch strings.ToUpper(seqStr) {
		case "PARALLEL":
			//default option
		case "SEQUENTIAL":
			sequence = true
		default:
			return fmt.Errorf("expected MULTISOURCE_ORDER	 to be PARALLEL or SEQUENTIAL in transform %s but got '%s'", block.GetName(), seqStr)

		}

	}

	if sequence {
		transform.Sequence(sourceSequence)
	}

	return nil
}

//addPlugin adds the plugin Transform to the dag.
// As of current release:
// - Limited to shell plugins only (built-in Python scripts not yet implemented).
//   This is not a hard limitation in the sense that Python plugins can still be written
//   and used, just not stored in the job as part of the transform body.
func addPlugin(js *aql.JobScript, dag engine.Coordinator, transform aql.Transform) (*plugins.Transform, error) {
	opts := transform.Options

	exec, ok := aql.FindOption(opts, "EXECUTABLE")

	if !ok {
		return nil, fmt.Errorf("expected EXECUTABLE option for transform %s", transform.Name)
	}

	execStr, ok := exec.String()

	if !ok {
		return nil, fmt.Errorf("expected EXECUTABLE option for transform %s to be a string", transform.Name)
	}

	args, ok := aql.FindOption(opts, "ARGS")

	var argStr string

	if ok {
		argStr2, ok2 := args.String()

		if !ok2 {
			return nil, fmt.Errorf("expected ARGS option for transform %s to be a string", transform.Name)
		}

		argStr = argStr2
	}

	var argList []string

	if ok {
		if err := json.Unmarshal([]byte(argStr), &argList); err != nil {
			return nil, fmt.Errorf("error parsing JSON for ARGS option in transform %s: %v", transform.Name, err)
		}
	}

	//Create plugin instance and configure with options
	sRPC := plugins.TransformJSONRPC{Path: execStr, Args: argList}
	s := plugins.Transform{
		Plugin: &sRPC,
		Alias:  transform.Name, //FIXME: What if it is a source for another block??
	}
	if err := s.Configure(transform.Options); err != nil {
		return nil, err
	}

	//FIXME: Transform aliases don't work. There are a few issues here:
	//  1) How does a transform know what its aliases are?
	//  2) If there are multiple aliases, how should this be dealt with?
	dag.AddTransform(strings.ToLower(transform.Name), strings.ToLower(transform.Name), &s)

	return &s, nil
}

//sources makes engine.Source s out of JobScript sources.
//As of current release:
//	- Limited to SQL sources (Excel sources require scripts or built-ins to process data which won't come until vNext)
//	- Queries limited to single source (this will probably remain a limitation for the foreseeable future)
func sources(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, params *engine.ParameterTable) error {
	for _, query := range js.Queries {
		if len(query.Sources) != 1 {
			return fmt.Errorf("queries must have exactly one source but %s has %v", query.Name, len(query.Sources))
		}
		if query.Sources[0].Global {
			g := engine.SQLSource{
				Driver:           globalDbDriver,
				ConnectionString: globalDbConnString,
				Query:            query.Content,
				ParameterTable:   params,
				ParameterNames:   query.Parameters,
			}
			//alias := alias(query.Sources[0], nil)
			alias := query.Name //Queries can only have one source, so let's do away with this confusing alias nonsense
			g.SetName(alias)
			dag.AddSource(strings.ToLower(query.Name), alias, &g)
			continue
		}
		if query.Sources[0].Database == nil {
			return fmt.Errorf("at present only GLOBAL, SCRIPT and CONNECTION sources are supported for query %s", query.Name)
		}
		if connMap[strings.ToLower(*query.Sources[0].Database)] == nil {
			return fmt.Errorf("could not find connection %s for query %s", *query.Sources[0].Database, query.Name)
		}
		conn := connMap[strings.ToLower(*query.Sources[0].Database)]
		s := engine.SQLSource{
			Driver:           conn.Driver,
			ConnectionString: conn.ConnectionString,
			Query:            query.Content,
			ParameterTable:   params,
			ParameterNames:   query.Parameters,
		}
		//alias := alias(query.Sources[0], conn)
		alias := query.Name //Queries can only have one source, so let's do away with this confusing alias nonsense
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
func sqlDest(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, block aql.Block, conn aql.Connection, dest aql.SourceSink) error {
	driver := conn.Driver
	connString := conn.ConnectionString

	tableOpt, ok := aql.FindOverridableOption("TABLE", conn.Name, block.GetOptions(), conn.Options)

	if !ok {
		return fmt.Errorf("expected TABLE option for connection %s in the connection definition or the block %s options", conn.Name, block.GetName())
	}

	table, ok := tableOpt.String()

	if !ok {
		return fmt.Errorf("expected TABLE option to be a STRING for connection %s and block %s", conn.Name, block.GetName())
	}
	alias := alias(dest, &conn)

	//Uniquify destination name
	dag.AddDestination(strings.ToLower(block.GetName()+destinationUniquifier+conn.Name), alias, &engine.SQLDestination{
		Name:             block.GetName() + destinationUniquifier + conn.Name,
		Driver:           driver,
		ConnectionString: connString,
		Table:            table,
		Alias:            alias,
	})

	dag.Connect(strings.ToLower(block.GetName()), strings.ToLower(block.GetName()+destinationUniquifier+conn.Name))

	return nil

}

//TODO: refactor all this option parsing nonsense
func globalDest(js *aql.JobScript, dag engine.Coordinator, block aql.Block, dest aql.SourceSink) error {
	driver := globalDbDriver
	connString := globalDbConnString

	tableOpt, ok := aql.FindOption(block.GetOptions(), "TABLE")

	if !ok {
		return fmt.Errorf("expected TABLE option for GLOBAL connection in the block %s options", block.GetName())
	}

	table, ok := tableOpt.String()

	if !ok {
		return fmt.Errorf("expected TABLE option to be a STRING for GLOBAL connection and block %s", block.GetName())
	}

	alias := alias(dest, nil)

	//Uniquify destination name
	dag.AddDestination(strings.ToLower(block.GetName()+destinationUniquifier+"GLOBAL"), alias, &engine.SQLDestination{
		Name:             block.GetName() + destinationUniquifier + "GLOBAL",
		Driver:           driver,
		ConnectionString: connString,
		Table:            table,
		Alias:            alias,
	})

	dag.Connect(strings.ToLower(block.GetName()), strings.ToLower(block.GetName()+destinationUniquifier+"GLOBAL"))

	return nil

}

//TODO: refactor all this option parsing nonsense
func excelDest(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, block aql.Block, conn aql.Connection, dest aql.SourceSink) error {
	//register Excel destination

	fileOpt, ok := aql.FindOption(conn.Options, "FILE")
	if !ok {
		return fmt.Errorf("connection %s should specify FILE option", conn.Name)
	}

	file, ok := fileOpt.String()

	if !ok {
		return fmt.Errorf("expected FILE option to be a STRING for connection %s and block %s", conn.Name, block.GetName())
	}

	sheetOpt, ok := aql.FindOverridableOption("SHEET", conn.Name, block.GetOptions(), conn.Options)

	if !ok {
		return fmt.Errorf("expected SHEET option for connection %s in the connection definition or the block %s options", conn.Name, block.GetName())
	}

	sheet, ok := sheetOpt.String()

	if !ok {
		return fmt.Errorf("expected SHEET option to be a STRING for connection %s and block %s", conn.Name, block.GetName())
	}

	templateOpt, ok := aql.FindOverridableOption("TEMPLATE", conn.Name, block.GetOptions(), conn.Options)

	var template string
	if ok {
		var ok2 bool
		template, ok2 = templateOpt.String()
		if !ok2 {
			return fmt.Errorf("expected TEMPLATE option to be a STRING for connection %s and query %s", conn.Name, block.GetName())
		}

	}

	overwriteOpt, ok := aql.FindOverridableOption("OVERWRITE", conn.Name, block.GetOptions(), conn.Options)

	var overwrite bool
	if ok {
		overwrite = overwriteOpt.Truthy()
	}

	rangOpt, ok := aql.FindOverridableOption("RANGE", conn.Name, block.GetOptions(), conn.Options)

	if !ok {
		return fmt.Errorf("expected RANGE option for connection %s in the connection definition or the query %s options", conn.Name, block.GetOptions())
	}

	rang, ok := rangOpt.String()

	if !ok {
		return fmt.Errorf("expected RANGE option to be a STRING for connection %s and query %s", conn.Name, block.GetName())
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
		columns   []string
	)

	trs, ok := aql.FindOverridableOption("TRANSPOSE", conn.Name, block.GetOptions(), conn.Options)

	if ok {
		transpose = trs.Truthy()
	}

	colsOpt, ok := aql.FindOverridableOption("COLUMNS", conn.Name, block.GetOptions(), conn.Options)

	if ok {
		cols, ok2 := colsOpt.String()
		if !ok2 {
			return fmt.Errorf("expected COLUMNS option to be a STRING for connection %s and query %s", conn.Name, block.GetName())
		}
		columns = strings.Split(cols, ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	alias := alias(dest, &conn)
	//Make destination name unique
	dag.AddDestination(strings.ToLower(block.GetName()+destinationUniquifier+conn.Name), alias, &engine.ExcelDestination{
		Name:     block.GetName() + destinationUniquifier + conn.Name,
		Filename: file,
		Sheet:    sheet,
		Range: engine.ExcelRange{
			X1: x1,
			Y1: y1,
			X2: xx2,
			Y2: yy2,
		},
		Transpose: transpose,
		Cols:      columns,
		Overwrite: overwrite,
		Template:  template,
		Alias:     alias,
	})
	dag.Connect(strings.ToLower(block.GetName()), strings.ToLower(block.GetName()+destinationUniquifier+conn.Name))

	return nil

}

func excelSource(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, transform aql.Transform, conn aql.Connection, source aql.SourceSink) error {
	//register Excel destination

	fileOpt, ok := aql.FindOption(conn.Options, "FILE")
	if !ok {
		return fmt.Errorf("connection %s should specify FILE option", conn.Name)
	}

	file, ok := fileOpt.String()

	if !ok {
		return fmt.Errorf("expected FILE option to be a STRING for connection %s and transform %s", conn.Name, transform.Name)
	}

	sheetOpt, ok := aql.FindOverridableOption("SHEET", conn.Name, transform.Options, conn.Options)

	if !ok {
		return fmt.Errorf("expected SHEET option for connection %s in the connection definition or the transform %s options", conn.Name, transform.Name)
	}

	sheet, ok := sheetOpt.String()

	if !ok {
		return fmt.Errorf("expected SHEET option to be a STRING for connection %s and transform %s", conn.Name, transform.Name)
	}

	rangOpt, ok := aql.FindOverridableOption("RANGE", conn.Name, transform.Options, conn.Options)

	if !ok {
		return fmt.Errorf("expected RANGE option for connection %s in the connection definition or the transform %s options", conn.Name, transform.Name)
	}

	rang, ok := rangOpt.String()

	if !ok {
		return fmt.Errorf("expected RANGE option to be a STRING for connection %s and query %s", conn.Name, transform.Name)
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

	var columns []string

	colsOpt, ok := aql.FindOverridableOption("COLUMNS", conn.Name, transform.Options, conn.Options)

	if ok {
		cols, ok2 := colsOpt.String()
		if !ok2 {
			return fmt.Errorf("expected COLUMNS option to be a STRING for connection %s and transform %s", conn.Name, transform.Name)
		}
		columns = strings.Split(cols, ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	alias := alias(source, &conn)

	//Make destination name unique
	dag.AddSource(strings.ToLower(transform.Name+sourceUniquifier+alias), alias, &engine.ExcelSource{
		Name:     transform.Name + sourceUniquifier + alias,
		Filename: file,
		Sheet:    sheet,
		Range: engine.ExcelRange{
			X1: x1,
			Y1: y1,
			X2: xx2,
			Y2: yy2,
		},
		Cols: columns,
	})

	//dag.Connect(strings.ToLower(transform.Name+sourceUniquifier+alias), strings.ToLower(transform.Name))

	return nil

}

func parameterDest(js *aql.JobScript, dag engine.Coordinator, query *aql.Query, dest aql.SourceSink, p *engine.ParameterTable) error {
	paramDest := engine.NewParameterTableDestination(p, dest.Variables)
	name := strings.ToLower(query.Name + destinationUniquifier + engine.ParameterTableName)
	err := dag.AddDestination(name, engine.ParameterTableName, paramDest)

	if err != nil {
		return err
	}

	return dag.Connect(strings.ToLower(query.Name), name)
}

//destinations makes engine.Destination s out of JobScript destinations and connects sources to them.
//As of current release:
//  - Limited to SQL, parameter or Excel destinations
//  - Multiple destinations supported for queries. The table for multiple destinations needs to be specified as TABLE_{DEST_NAME} = '{TABLE_NAME}'
//  - GLOBAL, SCRIPT and BLOCK destinations not supported
func destinations(js *aql.JobScript, dag engine.Coordinator, connMap map[string]*aql.Connection, p *engine.ParameterTable) error {
	for _, query := range js.Queries {
		for _, dest := range query.Destinations {
			if dest.Variables != nil {
				if err := parameterDest(js, dag, &query, dest, p); err != nil {
					return err
				}
				continue
			}
			if dest.Block != nil {
				return fmt.Errorf("BLOCK destinations are not allowed because they create non-deterministic source orders: %s", query.Name)
			}

			if dest.Global {
				if err := globalDest(js, dag, &query, dest); err != nil {
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
				err = excelDest(js, dag, connMap, &query, conn, dest)
			} else {
				err = sqlDest(js, dag, connMap, &query, conn, dest)
			}
			if err != nil {
				return err
			}

		}
	}
	for _, transform := range js.Transforms {
		for _, dest := range transform.Destinations {
			if dest.Block != nil {
				return fmt.Errorf("BLOCK destinations are not allowed because they create non-deterministic source orders: %s", transform.Name)
			}

			if dest.Global {
				if err := globalDest(js, dag, &transform, dest); err != nil {
					return err
				}
				continue
			}
			if dest.Database != nil && connMap[strings.ToLower(*dest.Database)] == nil {
				return fmt.Errorf("destination %s not found for query %s", *dest.Database, transform.Name)
			}
			conn := *connMap[strings.ToLower(*dest.Database)]
			var err error
			if strings.ToUpper(conn.Driver) == "EXCEL" {
				err = excelDest(js, dag, connMap, &transform, conn, dest)
			} else {
				err = sqlDest(js, dag, connMap, &transform, conn, dest)
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
