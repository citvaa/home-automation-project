import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { ChangeDetectorRef, Component, OnDestroy, OnInit, inject } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { Subscription, catchError, interval, of, startWith, switchMap } from 'rxjs';

type ElementKey = 'DS1' | 'DPIR1' | 'DUS1' | 'DMS' | 'DL' | 'DB';

interface ElementState {
  value: string | number | boolean | null;
  timestamp: string | null;
  unit: string | null;
  bucket: string;
}

interface StatusResponse {
  device_id: string;
  updated_at: string;
  elements: Partial<Record<ElementKey, ElementState | null>>;
}

interface UiElement {
  key: ElementKey;
  label: string;
  displayValue: string;
  unit: string;
  timestamp: string;
}

@Component({
  selector: 'app-root',
  imports: [CommonModule],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App implements OnInit, OnDestroy {
  private readonly http = inject(HttpClient);
  private readonly sanitizer = inject(DomSanitizer);
  private readonly cdr = inject(ChangeDetectorRef);

  private pollSub?: Subscription;

  readonly apiBase = 'http://localhost:5000';
  readonly grafanaBase = 'http://localhost:3000';
  readonly deviceId = 'PI1';
  readonly grafanaUrl: SafeResourceUrl;

  loading = true;
  errorMessage = '';
  updatedAt = '-';

  elements: UiElement[] = [
    { key: 'DS1', label: 'Door Sensor (DS1)', displayValue: 'N/A', unit: '', timestamp: '-' },
    { key: 'DPIR1', label: 'Motion Sensor (DPIR1)', displayValue: 'N/A', unit: '', timestamp: '-' },
    { key: 'DUS1', label: 'Ultrasonic Sensor (DUS1)', displayValue: 'N/A', unit: '', timestamp: '-' },
    { key: 'DMS', label: 'Membrane Switch (DMS)', displayValue: 'N/A', unit: '', timestamp: '-' },
    { key: 'DL', label: 'LED (DL)', displayValue: 'N/A', unit: '', timestamp: '-' },
    { key: 'DB', label: 'Buzzer (DB)', displayValue: 'N/A', unit: '', timestamp: '-' }
  ];

  constructor() {
    this.grafanaUrl = this.sanitizer.bypassSecurityTrustResourceUrl(
      `${this.grafanaBase}/d/home-automation/home-automation?orgId=1&kiosk`
    );
  }

  ngOnInit(): void {
    this.pollSub = interval(5000)
      .pipe(
        startWith(0),
        switchMap(() =>
          this.http.get<StatusResponse>(`${this.apiBase}/status/${this.deviceId}`).pipe(
            catchError(() => {
              this.errorMessage = 'Ne mogu da učitam stanje uređaja sa servera.';
              return of(null);
            })
          )
        )
      )
      .subscribe((response) => {
        this.loading = false;
        if (!response) {
          this.cdr.detectChanges();
          return;
        }

        this.errorMessage = '';
        this.updatedAt = this.formatTime(response.updated_at);

        this.elements = this.elements.map((element) => {
          const latest = response.elements[element.key] ?? null;
          if (!latest) {
            return { ...element, displayValue: 'N/A', unit: '', timestamp: '-' };
          }
          return {
            ...element,
            displayValue: this.toDisplayValue(element.key, latest.value),
            unit: latest.unit ?? '',
            timestamp: latest.timestamp ? this.formatTime(latest.timestamp) : '-'
          };
        });

        this.cdr.detectChanges();
      });
  }

  ngOnDestroy(): void {
    this.pollSub?.unsubscribe();
  }

  private toDisplayValue(key: ElementKey, value: string | number | boolean | null): string {
    if (value === null || value === undefined) {
      return 'N/A';
    }

    if (key === 'DL' || key === 'DB') {
      const isOn = value === true || value === 1 || value === '1' || `${value}`.toLowerCase() === 'true';
      return isOn ? 'ON' : 'OFF';
    }

    if (key === 'DS1' || key === 'DPIR1' || key === 'DMS') {
      const active = value === true || value === 1 || value === '1' || `${value}`.toLowerCase() === 'true';
      return active ? 'ACTIVE' : 'INACTIVE';
    }

    return `${value}`;
  }

  private formatTime(timestamp: string): string {
    const dt = new Date(timestamp);
    if (Number.isNaN(dt.getTime())) {
      return timestamp;
    }
    return dt.toLocaleString();
  }
}
