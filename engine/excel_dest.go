package engine

import (
	"fmt"
	xlsx "github.com/360EntSecGroup-Skylar/excelize"
	"os"
	"time"
)

type ExcelDestination struct {
	Name      string
	Filename  string
	Overwrite bool
	Template  string
	Sheet     string
	Range     ExcelRange
	Alias     string
	Transpose bool
	Cols      []string
	posY      int
	posX      int
}

func (ed *ExcelDestination) Ping() error {
	if fileExists(ed.Filename) && !ed.Overwrite {
		return fmt.Errorf("destination file %s already exists and OVERWRITE is false", ed.Filename)
	}
	if ed.Range.X2.N && ed.Range.Y2.N {
		return ErrExcelTooManyWildcards
	}
	if ed.Template != "" {
		if _, err := os.Stat(ed.Template); err != nil {
			return err
		}

	}
	return nil
}

func (ed *ExcelDestination) log(l Logger, level LogLevel, msg string) {
	l.Chan() <- Event{
		Time:    time.Now(),
		Source:  ed.Name,
		Message: msg,
		Level:   level,
	}
}

func (ed *ExcelDestination) fatalerr(err error, st Stream, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  ed.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
}

func (ed *ExcelDestination) copyTemplateToDestination() error {
	if ed.Template != "" {
		return copyFile(ed.Template, ed.Filename)
	}
	return nil
}

func (ed *ExcelDestination) Open(s Stream, l Logger, st Stopper) {
	if err := ed.copyTemplateToDestination(); err != nil {
		ed.fatalerr(err, s, l)
		return
	}
	err := fileManager.Register(ed.Filename, ed.Template == "")
	if err != nil {
		ed.fatalerr(err, s, l)
		return
	}

	if ed.Template == "" {
		//we may have to create sheet

		fileManager.Use(ed.Filename, func(f *xlsx.File) {
			if _, ok := f.Sheet[ed.Sheet]; !ok {
				f.NewSheet(ed.Sheet)
			}
		})
	}
	ed.log(l, Info, "Excel destination opened")
	var (
		colMappers []func([]interface{}) interface{}
	)

	ed.posX = ed.Range.X1
	ed.posY = ed.Range.Y1
	var counter int
	for msg := range s.Chan(ed.Alias) {
		counter++
		ed.log(l, Trace, fmt.Sprintf("Found columns %v", s.Columns()))
		if st.Stopped() {
			ed.log(l, Warning, "Excel destination aborted")
			return
		}
		ed.log(l, Trace, fmt.Sprintf("Row %v", msg.Data))
		if colMappers == nil {
			for i := range ed.Cols {
				cm, err := getValue(s.Columns(), ed.Cols[i])
				if err != nil {
					ed.fatalerr(err, s, l)
					return
				}
				colMappers = append(colMappers, cm)
			}
		}
		var colLength int
		if ed.Transpose {
			colLength = ed.posY - ed.Range.Y1 + 1
		} else {
			colLength = len(msg.Data)
		}
		if !ed.Range.X2.N && ed.Range.X2.P-ed.Range.X1+1 != colLength {
			ed.fatalerr(fmt.Errorf("wrong number of columns. Expected %v columns, got %v", ed.Range.X2.P-ed.Range.X1+1, len(msg.Data)), s, l)
			return
		}
		if !ed.Range.Y2.N && ed.posY > ed.Range.Y2.P {
			ed.fatalerr(fmt.Errorf("range overflow: too many rows. Expected %v rows", ed.Range.Y2.P), s, l)
			return
		}
		fileManager.Use(ed.Filename, func(f *xlsx.File) {
			for i := range msg.Data {
				if colMappers != nil {
					f.SetCellValue(ed.Sheet, pointToCol(ed.posX, ed.posY), colMappers[i](msg.Data))
				} else {
					//identity mapping
					f.SetCellValue(ed.Sheet, pointToCol(ed.posX, ed.posY), msg.Data[i])
				}

				if ed.Transpose {
					ed.posY++
				} else {
					ed.posX++
				}
			}
			if ed.Transpose {
				ed.posY = ed.Range.Y1
				ed.posX++
			} else {
				ed.posX = ed.Range.X1
				ed.posY++
			}
		})
	}
	//TODO: Should we be strict with the range or just check for overflows above?
	//Here, if the range is a subset of the declared range, we are ignoring it.
	//Maybe there should be the option of being strict, which is useful as a test
	//condition to ensure that no rows/columns are missed.
	fileManager.Use(ed.Filename, func(f *xlsx.File) {
		var err error
		if ed.Template == "" {
			err = f.SaveAs(ed.Filename)
		} else {
			err = f.Save()
		}
		if err != nil {
			ed.fatalerr(fmt.Errorf("error saving file %v", err), s, l)
		}
	})

	ed.log(l, Info, "Excel destination closed")
}
