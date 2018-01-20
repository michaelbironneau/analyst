import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  templateUrl: 'dashboard.component.html',
  styles: ['monaco-editor { height: 500px; }']
})
export class DashboardComponent {
  editorOptions = {theme: 'vs-dark', language: 'javascript'};
  code: string= 'function x() {\nconsole.log("Hello world!");\n}';
  constructor( ) { }

}
