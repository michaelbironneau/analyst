import { NgModule } from '@angular/core';
import { ChartsModule } from 'ng2-charts/ng2-charts';

import { DashboardComponent } from './dashboard.component';
import { DashboardRoutingModule } from './dashboard-routing.module';
import { MonacoEditorModule } from 'ngx-monaco';

@NgModule({
  imports: [
    DashboardRoutingModule,
    ChartsModule,
    MonacoEditorModule,
    MonacoEditorModule.forRoot()
  ],
  declarations: [ DashboardComponent]
})
export class DashboardModule { }
