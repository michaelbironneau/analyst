import {Directive, ElementRef, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChanges} from '@angular/core';
import {EditorFromTextArea, EditorConfiguration, fromTextArea} from 'codemirror';
import { defineAQLMode } from './codemirror.mode';
import * as CodeMirror from 'codemirror';

@Directive({
  selector: '[codemirror]'
})
export class CodeMirrorDirective implements OnInit, OnChanges {

  @Input() content: string;
  @Input() config: EditorConfiguration = {
    lineNumbers: true,
    mode: 'aql'
  };
  @Output() onChange = new EventEmitter<{editorInstance: any, changes: any}>();
  editorRef: EditorFromTextArea;

  constructor(private element: ElementRef) {}

  ngOnInit() {
    defineAQLMode(CodeMirror);
    this.editorRef = fromTextArea(this.element.nativeElement, this.config);
    this.editorRef.setValue(this.content);
    this.editorRef.on('change', (cmInstance, event) => this.onChange.emit({editorInstance: cmInstance, changes: event}));
  }

  ngOnChanges(changes: SimpleChanges) {

    if (this.editorRef) {
      if (changes.content) {
        this.editorRef.setValue(this.content);
      }
      if (changes.config) {
        Object.keys(this.config).map(k => this.editorRef.setOption(k, this.config[k]));
      }
    }
  }

  getContent(): string {
    return this.editorRef.getValue();
  }

  setOption(key: string, value: any) {
    this.editorRef.setOption(key, value);
  }

}