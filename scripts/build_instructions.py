"""Build the submission instructions PDF (requires reportlab)."""
from pathlib import Path
from html import escape
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Preformatted, Table, TableStyle, PageBreak

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'output/pdf/GROUP20_INSTRUCTIONS.pdf'
OUT.parent.mkdir(parents=True,exist_ok=True)
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='MainTitle',fontName='Helvetica-Bold',fontSize=21,leading=25,spaceAfter=12,textColor=colors.black))
styles.add(ParagraphStyle(name='SectionTitle',fontName='Helvetica-Bold',fontSize=15,leading=19,spaceAfter=10,textColor=colors.black))
styles.add(ParagraphStyle(name='Copy',fontName='Helvetica',fontSize=10,leading=14,spaceAfter=8))
styles.add(ParagraphStyle(name='CodeBlock',fontName='Courier',fontSize=8,leading=11,spaceBefore=4,spaceAfter=10))
styles.add(ParagraphStyle(name='SmallCopy',fontName='Helvetica',fontSize=8.5,leading=11,spaceAfter=6))
story=[]

def text(value,style='Copy'): story.append(Paragraph(escape(value),styles[style]))
def heading(value): text(value,'SectionTitle')
def code(value): story.append(Preformatted(value,styles['CodeBlock']))
def page(title): story.append(PageBreak());heading(title)
def table(rows,widths):
 data=[[Paragraph(escape(str(c)),styles['SmallCopy']) for c in r] for r in rows]
 t=Table(data,colWidths=widths,repeatRows=1,hAlign='LEFT')
 t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e8edf2')),('GRID',(0,0),(-1,-1),.5,colors.HexColor('#d9d9d9')),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6)]))
 story.append(t);story.append(Spacer(1,10))

text('CraneLock Deployment Instructions','MainTitle')
text('CSC3104 Group 20 | Platform and failure injection | 11 November 2026','SmallCopy')
heading('1 Scope and prerequisites')
text('These instructions deploy the native controller, trusted gate and three fixed-membership crane agents, with durable storage, mutual TLS for gate/controller connections and provisioned monitoring. The platform supports local Docker Compose and a separate single-node Kubernetes experiment.')
text('Use Python 3.12 or newer, Git and Docker Desktop with Linux containers. Run commands from the repository root. Kubernetes additionally requires kind and kubectl. Allow memory and disk space for a local Kubernetes node and image imports; run one deployment at a time on a small laptop.')
text('Create a virtual environment once. macOS/Linux:')
code('python3.12 -m venv .venv\nsource .venv/bin/activate\npython -m pip install -e ".[dev]"')
text('Windows PowerShell:')
code('py -3.12 -m venv .venv\n.\\.venv\\Scripts\\Activate.ps1\npython -m pip install -e ".[dev]"')
text('Open Docker Desktop and wait for its engine. Validate the code:')
code('docker info\npython -m pytest -q')
text('There is no real crane hardware. The existing simulator CLI starts its own services unless adapted to use the external endpoints. A passing deployment check does not prove physical lane safety or scalability.')
heading('Development credentials')
text('Provisioning creates public configuration and private development keys under ignored demo-data/integrated. Each actor receives its own signing file. TLS client identities are shared per destination service for simulation; peer-agent RPCs use mutual TLS with one certificate per crane (tls/agents), and host tools use a read-only monitor certificate. Do not publish keys, delete persistent state or reuse these credentials in production.')

page('2 Docker setup and endpoints')
text('First-time setup only, on a new experiment and new Docker volumes:')
code('python scripts/integrated.py provision\npython scripts/integrated.py init\npython scripts/integrated.py up\npython scripts/integrated.py status\npython scripts/integrated.py check')
text('Provision refuses an existing experiment directory. Init refuses an existing database, explicitly starts the gate and installs controller-signed CENTRAL authority at epoch 0, revision 1, holder crane-1. Occupancy remains UNKNOWN and entry is blocked.')
text('For an existing experiment, including Arif\'s current laptop, skip provision/init/bootstrap:')
code('python scripts/integrated.py up\npython scripts/integrated.py state\npython scripts/integrated.py check')
text('The helper loads all three Compose files, including monitoring. Check verifies convergence and native controller heartbeat behavior. It is intended for a healthy CENTRAL baseline, not a fault currently in LOCAL mode. Initial DNS connections may need a short settling interval.')
table([['Service','Host endpoint','Internal endpoint'],['Gate gRPC','localhost:50061','gate:50051'],['Controller heartbeat','localhost:50062','controller:50052'],['Crane 1 gRPC (mTLS)','127.0.0.1:50101','crane-1:50051'],['Crane 2 gRPC (mTLS)','127.0.0.1:50102','crane-2:50051'],['Crane 3 gRPC (mTLS)','127.0.0.1:50103','crane-3:50051'],['Gate metrics','127.0.0.1:9190','gate:9090']],[48*mm,60*mm,60*mm])
text('Container names follow cranelock-integrated-SERVICE-1, e.g. cranelock-integrated-controller-1 and cranelock-integrated-crane-1-1. All host bindings are loopback-only. Gate/controller clients require their matching CA, client certificate and key.')
code('python scripts/integrated.py endpoints')
text('This prints endpoint and local credential paths, never private key values. Preserve the cranelock-integrated project name so the existing volumes are reused.')

page('3 Monitoring and failure controls')
text('Integrated Prometheus: http://127.0.0.1:9092. Integrated Grafana: http://127.0.0.1:3001. The data source and dashboard are provisioned automatically. Username: admin. Read the initial local password from demo-data/integrated/grafana-password; never include it in evidence or Git.')
text('Five targets cover the gate, controller and three agents. Panels show scrape reachability, gate health/occupancy, cached ownership/epochs/modes, queues, suspicions, local recovery events, controller heartbeats and gate counters. Recovery durations measure local suspicion to authority installation, not physical safe entry. Process counters reset on restart.')
text('A target can be DOWN even when the last reported gate-health sample was healthy. The dashboard uses current scrape status and instant queries. UNKNOWN occupancy is a healthy fail-closed state, not evidence that the process needs restarting.')
heading('Single-service actions')
code('python scripts/integrated.py stop controller\npython scripts/integrated.py start controller\npython scripts/integrated.py kill crane-1\npython scripts/integrated.py start crane-1\npython scripts/integrated.py restart gate')
text('Stop is graceful and kill is SIGKILL. Actions target exactly one container; restarting an agent must not implicitly restart a deliberately stopped controller. Query current authority before choosing the holder. Restarts retain saved state and reconcile; they never clear the physical lane.')
heading('Timed transport and message faults')
code('python scripts/faults.py build\npython scripts/faults.py partition crane-1 --seconds 10\npython scripts/faults.py partition all --seconds 10\npython scripts/faults.py delay crane-3 --ms 200 --reorder 25\npython scripts/faults.py reordered-command')
text('The network helper uses NET_ADMIN only in a selected container network namespace. Fault duration is bounded and cleanup runs afterward. Never overlap faults or restart a target during injection. Netem reorders packets; TCP still preserves byte order. The reordered-command case deliberately releases an already signed old grant after a newer controller authority revision and expects rejection.')
text('Do not run those synthetic authority tests alongside an external workload runner. Incomplete cleanup or failed validation must be investigated before repeating a run.')

page('4 Kubernetes deployment')
text('Install kind and kubectl. The helper uses a dedicated kind-cranelock context and demo-data/kubeconfig, never the default context. Provision the same local configuration and build images through the Docker steps first. Kubernetes has its own gate/agent PVCs; its authority is independent of Compose.')
code('python scripts/kubernetes.py cluster-create\npython scripts/kubernetes.py load-images\npython scripts/kubernetes.py init\npython scripts/kubernetes.py up\npython scripts/kubernetes.py status\npython scripts/kubernetes.py check\npython scripts/validate_kubernetes.py')
text('The loader exports only the local image architecture to avoid Docker Desktop multi-platform import errors. Init is for new PVCs only and runs explicit initialization/bootstrap Jobs before normal workloads. If interrupted, inspect Jobs, logs and database state; do not reset or run initialization over an existing experiment.')
text('Kustomize resources are generated in deploy/kubernetes/integrated. Public configuration is a ConfigMap. Private material is applied as namespace Secrets through stdin, never embedded in tracked resources or CLI arguments. Each crane has a one-replica StatefulSet, a headless Service and an independent volumeClaimTemplate. The gate has one replica, Recreate and one PVC.')
text('Readiness, startup and liveness probes check responsiveness; UNKNOWN occupancy and REJOINING are valid states. Headless peer services publish not-yet-ready addresses to avoid discovery deadlock. Storage and node-loss behavior outside the tested local filesystem are not established.')
heading('Resume without reinitializing')
code('python scripts/kubernetes.py cluster-start\npython scripts/kubernetes.py up\npython scripts/kubernetes.py check')
text('To view Kubernetes Grafana, use a separate terminal:')
code('kubectl --kubeconfig demo-data/kubeconfig \\\n  --context kind-cranelock -n cranelock \\\n  port-forward service/grafana 3002:3000')
text('Open http://127.0.0.1:3002 and stop forwarding with Ctrl+C. Internal service names/ports match Compose; no Kubernetes NodePort or public ingress is created.')
text('Validation replaces a crane pod while preserving its PVC and checks rejoin. It separately restarts the gate during planned maintenance to verify exact authority retention, a new session and invalidated clearance, then checks peer convergence and monitoring. This is not a node-loss or replicated-gate experiment.')

page('5 Evidence and safe shutdown')
heading('Repeatable deployment checks')
code('python scripts/validate_platform.py all --new-empty-simulation\npython scripts/external_smoke.py --new-empty-simulation')
text('The new-empty-simulation flag explicitly permits a synthetic, initially empty lane test when gate occupancy is UNKNOWN and no grant exists. It reports one fresh observation, suppresses updates beyond the timeout and verifies LANE_UNKNOWN blocks new grants. No physical movement is attempted. Never use it to overwrite an existing physical workload\'s occupancy.')
text('The external smoke example adds one token handoff and serialized synthetic entry/exit through the actual container endpoints, leaving occupancy UNKNOWN afterward. The serial validator covers controller fallback/return, LOCAL holder crash and fencing, gate outage, restart/rejoin, monitoring DOWN visibility, peer partition recovery and packet delay cleanup. The delayed signed-message check is also available individually. Validators retain failures as evidence rather than converting them into successes.')
code('python scripts/integrated.py logs\npython scripts/kubernetes.py logs')
text('Evidence is stored under ignored run/platform-validation: timestamped fault-events.jsonl, unique validation JSON and cumulative log exports. Export logs before recreating containers. Command timestamps bracket execution; derive precise detection/safe-entry latency from correlated component and physical events. Do not subtract clocks from unrelated hosts or assume packet timing is deterministic.')
heading('Stop everything belonging to this project')
code('python scripts/integrated.py stop\npython scripts/kubernetes.py stop\npython scripts/kubernetes.py cluster-stop')
text('If the older standalone monitoring/agent projects are running, stop those separately using their Compose files. Then quit Docker Desktop if no other work needs it. These commands retain volumes and the stopped kind node. Never use down -v, delete PVCs/databases, or delete the cluster to resume an experiment.')
heading('Troubleshooting and workload handoff')
text('If an agent fails startup, check its identity, full peer membership, signing key, mounted paths and TLS names. Outside-loopback gate/controller connections require all TLS files. If a test cannot bind localhost in a sandbox, run it in the normal project terminal. If a stored gate epoch is older than an agent file, investigate mixed experiment state rather than forcing rollback.')
text('Darius must connect the workload runner to the published endpoints instead of constructing in-process services. Coordinate simulator-signed observations, crane-signed grant requests and the serialized physical lane boundary with the component owners. Final physical safety, workload latency, throughput and scalability remain shared integration/evaluation work.')
text('Detailed reference: docs/PLATFORM_HANDOFF.md and docs/PLATFORM_OPERATIONS.md. Report material: docs/PLATFORM_IMPLEMENTATION.md. Actual validation outcomes: docs/Platform_Validation.md. No production or industrial certification is claimed.','SmallCopy')


def footer(canvas,doc):
 canvas.saveState();canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#555555'))
 canvas.drawString(20*mm,12*mm,'CraneLock | Group 20 | Deployment instructions')
 canvas.drawRightString(190*mm,12*mm,str(doc.page));canvas.restoreState()

SimpleDocTemplate(str(OUT),pagesize=A4,rightMargin=20*mm,leftMargin=20*mm,topMargin=18*mm,bottomMargin=20*mm,title='CraneLock Deployment Instructions',author='Group 20').build(story,onFirstPage=footer,onLaterPages=footer)
print(OUT)
