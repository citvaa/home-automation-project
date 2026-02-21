import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { ChangeDetectorRef, Component, OnDestroy, OnInit, inject } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { Subscription, catchError, interval, of, startWith, switchMap } from 'rxjs';

type ElementKey = 'DS1' | 'DPIR1' | 'DUS1' | 'DMS' | 'DL' | 'DB' | 'WEBC';

interface ElementState {
  value: string | number | boolean | null;
  timestamp: string | null;
  unit: string | null;
  bucket: string;
}

interface SecurityState {
  mode: string;
  occupancy: number;
  alarm_reason: string | null;
}

interface StatusResponse {
  device_id: string;
  updated_at: string;
  elements: Partial<Record<ElementKey, ElementState | null>>;
  security?: SecurityState;
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
  readonly devices = ['PI1', 'PI2', 'PI3'];
  selectedDeviceId = 'PI1';
  readonly grafanaUrl: SafeResourceUrl;

  loading = true;
  errorMessage = '';
  updatedAt = '-';

  securityMode = 'DISARMED';
  alarmReason: string | null = null;
  occupancy = 0;
  pinInput = '';
  pinError = '';
  pinBusy = false;
  cameraUrl = '';

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
          this.http.get<StatusResponse>(`${this.apiBase}/status/${this.selectedDeviceId}`).pipe(
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

        const security = response.security;
        if (security) {
          this.securityMode = security.mode || 'DISARMED';
          this.occupancy = Number.isFinite(security.occupancy) ? security.occupancy : 0;
          this.alarmReason = security.alarm_reason ?? null;
        } else {
          this.securityMode = 'DISARMED';
          this.occupancy = 0;
          this.alarmReason = null;
        }

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

        this.cameraUrl = this.toCameraUrl(response.elements.WEBC ?? null);

        this.cdr.detectChanges();
      });
  }

  onDeviceChange(deviceId: string): void {
    if (!deviceId || this.selectedDeviceId === deviceId) {
      return;
    }
    this.selectedDeviceId = deviceId;
    this.loading = true;
    this.errorMessage = '';
  }

  get alarmActive(): boolean {
    return this.securityMode === 'ALARM';
  }

  armSystem(): void {
    if (this.pinBusy) {
      return;
    }
    this.pinBusy = true;
    this.pinError = '';
    this.http
      .post<{ ok: boolean; error?: string }>(`${this.apiBase}/security/${this.selectedDeviceId}/arm`, {
        pin: this.pinInput
      })
      .pipe(
        catchError(() => {
          this.pinError = 'Ne mogu da aktiviram sistem.';
          return of({ ok: false, error: 'error' });
        })
      )
      .subscribe((res) => {
        this.pinBusy = false;
        if (!res.ok) {
          this.pinError = res.error || 'Neispravan PIN.';
        } else {
          this.pinInput = '';
        }
        this.cdr.detectChanges();
      });
  }

  disarmSystem(): void {
    if (this.pinBusy) {
      return;
    }
    this.pinBusy = true;
    this.pinError = '';
    this.http
      .post<{ ok: boolean; error?: string }>(`${this.apiBase}/security/${this.selectedDeviceId}/disarm`, {
        pin: this.pinInput
      })
      .pipe(
        catchError(() => {
          this.pinError = 'Ne mogu da deaktiviram sistem.';
          return of({ ok: false, error: 'error' });
        })
      )
      .subscribe((res) => {
        this.pinBusy = false;
        if (!res.ok) {
          this.pinError = res.error || 'Neispravan PIN.';
        } else {
          this.pinInput = '';
        }
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

    if (key === 'DUS1') {
      const numeric = typeof value === 'number' ? value : Number(value);
      if (!Number.isNaN(numeric)) {
        return numeric.toFixed(3);
      }
    }

    return `${value}`;
  }

  private toCameraUrl(latest: ElementState | null): string {
    if (!latest || latest.value === null || latest.value === undefined) {
      return '';
    }
    if (typeof latest.value !== 'string') {
      return '';
    }
    if (latest.value.startsWith('b64:')) {
      const mime = latest.unit || 'image/jpeg';
      const payload = latest.value.slice(4);
      return `data:${mime};base64,${payload}`;
    }
    if (latest.value.startsWith('data:') || latest.value.startsWith('http')) {
      return latest.value;
    }
    return '';
  }

  private formatTime(timestamp: string): string {
    const dt = new Date(timestamp);
    if (Number.isNaN(dt.getTime())) {
      return timestamp;
    }
    return dt.toLocaleString();
  }
}
