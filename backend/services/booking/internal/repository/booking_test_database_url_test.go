package repository

import (
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestIsIsolatedBookingTestDatabaseURL(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want bool
	}{
		{name: "legacy Windows loopback mapping", dsn: "postgres://ticket_e2e:local@127.0.0.1:25435/ticketbox_booking_test?sslmode=disable", want: true},
		{name: "unapproved loopback port", dsn: "postgres://ticket_e2e:local@127.0.0.1:25439/ticketbox_booking_test?sslmode=disable", want: false},
		{name: "verified Linux private container target", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/ticketbox_booking_test?sslmode=disable", want: true},
		{name: "public database target", dsn: "postgres://ticket_e2e:local@8.8.8.8:5432/ticketbox_booking_test?sslmode=disable", want: false},
		{name: "unverified DNS target", dsn: "postgres://ticket_e2e:local@ticket-e2e-postgres:5432/ticketbox_booking_test?sslmode=disable", want: false},
		{name: "private target wrong port", dsn: "postgres://ticket_e2e:local@172.24.0.4:25435/ticketbox_booking_test?sslmode=disable", want: false},
		{name: "loopback without an approved mapping", dsn: "postgres://ticket_e2e:local@127.0.0.1:5432/ticketbox_booking_test?sslmode=disable", want: false},
		{name: "wrong database", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/postgres?sslmode=disable", want: false},
		{name: "query host override", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/ticketbox_booking_test?host=8.8.8.8", want: false},
		{name: "query private host override", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/ticketbox_booking_test?host=172.24.0.9", want: false},
		{name: "query port override", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/ticketbox_booking_test?port=5433", want: false},
		{name: "query database override", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/ticketbox_booking_test?database=postgres", want: false},
		{name: "query dbname override", dsn: "postgres://ticket_e2e:local@172.24.0.4:5432/ticketbox_booking_test?dbname=postgres", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isIsolatedBookingTestDatabaseURL(tt.dsn); got != tt.want {
				t.Fatalf("isIsolatedBookingTestDatabaseURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsVerifiedTicketE2EPostgresTarget(t *testing.T) {
	project := "rushgo-ticket-e2e-20261026-0123456789ab"
	containerID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	container := `[{"Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","State":{"Running":true},"Config":{"Labels":{"com.docker.compose.project":"rushgo-ticket-e2e-20261026-0123456789ab","com.docker.compose.service":"ticket-e2e-postgres","rushgo.ticket_e2e.project":"rushgo-ticket-e2e-20261026-0123456789ab"}},"HostConfig":{"PublishAllPorts":false,"PortBindings":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]}},"NetworkSettings":{"Ports":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]},"Networks":{"rushgo-ticket-e2e-20261026-0123456789ab_ticket-e2e":{"IPAddress":"172.28.0.2"}}}}]`
	network := `[{"Name":"rushgo-ticket-e2e-20261026-0123456789ab_ticket-e2e","Internal":true,"Labels":{"com.docker.compose.project":"rushgo-ticket-e2e-20261026-0123456789ab","com.docker.compose.network":"ticket-e2e","rushgo.ticket_e2e.project":"rushgo-ticket-e2e-20261026-0123456789ab"}}]`
	dsn := "postgres://ticket_e2e:local@172.28.0.2:5432/ticketbox_booking_test?sslmode=disable"
	if !isVerifiedTicketE2EPostgresTarget(dsn, project, containerID, []byte(container), []byte(network)) {
		t.Fatal("the exact running Postgres container on the isolated internal project network was rejected")
	}
	loopbackDSN := "postgres://ticket_e2e:local@127.0.0.1:25435/ticketbox_booking_test?sslmode=disable"
	if !isVerifiedTicketE2EPostgresTarget(loopbackDSN, project, containerID, []byte(container), []byte(network)) {
		t.Fatal("the exact isolated Postgres container published only on Windows loopback was rejected")
	}
	publicBinding := strings.Replace(container, `"HostIp":"127.0.0.1"`, `"HostIp":"0.0.0.0"`, 1)
	if isVerifiedTicketE2EPostgresTarget(loopbackDSN, project, containerID, []byte(publicBinding), []byte(network)) {
		t.Fatal("a Postgres test container published beyond loopback was accepted")
	}

	tests := []struct {
		name          string
		dsn           string
		project       string
		containerID   string
		containerJSON string
		networkJSON   string
	}{
		{name: "arbitrary private host", dsn: "postgres://ticket_e2e:local@172.28.0.9:5432/ticketbox_booking_test?sslmode=disable", project: project, containerID: containerID, containerJSON: container, networkJSON: network},
		{name: "wrong project label", dsn: dsn, project: project, containerID: containerID, containerJSON: `[{"Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","State":{"Running":true},"Config":{"Labels":{"com.docker.compose.project":"other","com.docker.compose.service":"ticket-e2e-postgres","rushgo.ticket_e2e.project":"other"}},"NetworkSettings":{"Networks":{"rushgo-ticket-e2e-20261026-0123456789ab_ticket-e2e":{"IPAddress":"172.28.0.2"}}}}]`, networkJSON: network},
		{name: "non-internal network", dsn: dsn, project: project, containerID: containerID, containerJSON: container, networkJSON: `[{"Name":"rushgo-ticket-e2e-20261026-0123456789ab_ticket-e2e","Internal":false,"Labels":{"com.docker.compose.project":"rushgo-ticket-e2e-20261026-0123456789ab","rushgo.ticket_e2e.project":"rushgo-ticket-e2e-20261026-0123456789ab"}}]`},
		{name: "extra network attachment", dsn: dsn, project: project, containerID: containerID, containerJSON: `[{"Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","State":{"Running":true},"Config":{"Labels":{"com.docker.compose.project":"rushgo-ticket-e2e-20261026-0123456789ab","com.docker.compose.service":"ticket-e2e-postgres","rushgo.ticket_e2e.project":"rushgo-ticket-e2e-20261026-0123456789ab"}},"NetworkSettings":{"Networks":{"rushgo-ticket-e2e-20261026-0123456789ab_ticket-e2e":{"IPAddress":"172.28.0.2"},"bridge":{"IPAddress":"172.17.0.3"}}}}]`, networkJSON: network},
		{name: "wrong container ID", dsn: dsn, project: project, containerID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", containerJSON: container, networkJSON: network},
		{name: "stopped container", dsn: dsn, project: project, containerID: containerID, containerJSON: `[{"Id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","State":{"Running":false},"Config":{"Labels":{"com.docker.compose.project":"rushgo-ticket-e2e-20261026-0123456789ab","com.docker.compose.service":"ticket-e2e-postgres","rushgo.ticket_e2e.project":"rushgo-ticket-e2e-20261026-0123456789ab"}},"NetworkSettings":{"Networks":{"rushgo-ticket-e2e-20261026-0123456789ab_ticket-e2e":{"IPAddress":"172.28.0.2"}}}}]`, networkJSON: network},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			containerJSON := strings.Replace(tt.containerJSON, `"NetworkSettings":{"Networks":`, `"NetworkSettings":{"Ports":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]},"Networks":`, 1)
			if isVerifiedTicketE2EPostgresTarget(tt.dsn, tt.project, tt.containerID, []byte(containerJSON), []byte(tt.networkJSON)) {
				t.Fatal("unverified Booking test database target was accepted")
			}
		})
	}
}

func TestIsVerifiedTicketE2EPostgresPrivateTargetWithoutEffectiveHostPort(t *testing.T) {
	project := "rushgo-ticket-e2e-20261026-0123456789ab"
	id := strings.Repeat("a", 64)
	configured := `[{"Id":"` + id + `","State":{"Running":true},"Config":{"Labels":{"com.docker.compose.project":"` + project + `","com.docker.compose.service":"ticket-e2e-postgres","rushgo.ticket_e2e.project":"` + project + `"}},"HostConfig":{"PublishAllPorts":false,"PortBindings":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]}},"NetworkSettings":{"Ports":{},"Networks":{"` + project + `_ticket-e2e":{"IPAddress":"172.28.0.2"}}}}]`
	network := `[{"Name":"` + project + `_ticket-e2e","Internal":true,"Labels":{"com.docker.compose.project":"` + project + `","com.docker.compose.network":"ticket-e2e","rushgo.ticket_e2e.project":"` + project + `"}}]`
	dsn := "postgres://ticket_e2e:local@172.28.0.2:5432/ticketbox_booking_test?sslmode=disable"
	if !isVerifiedTicketE2EPostgresTarget(dsn, project, id, []byte(configured), []byte(network)) {
		t.Fatal("verified Linux private target requires configured binding, not an active host publication")
	}
	cases := []struct{ name, dsn, inspected string }{
		{"loopback still needs active binding", "postgres://ticket_e2e:local@127.0.0.1:25435/ticketbox_booking_test?sslmode=disable", configured},
		{"configured bind exposed publicly", dsn, strings.Replace(configured, `"HostIp":"127.0.0.1"`, `"HostIp":"0.0.0.0"`, 1)},
		{"publish all forbidden", dsn, strings.Replace(configured, `"PublishAllPorts":false`, `"PublishAllPorts":true`, 1)},
		{"unapproved configured port", dsn, strings.Replace(configured, `"HostPort":"25435"`, `"HostPort":"25436"`, 1)},
		{"no configured identity", dsn, strings.Replace(configured, `"PortBindings":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]}`, `"PortBindings":{}`, 1)},
		{"public effective bind", dsn, strings.Replace(configured, `"Ports":{}`, `"Ports":{"5432/tcp":[{"HostIp":"0.0.0.0","HostPort":"25435"}]}`, 1)},
		{"unexpected effective port", dsn, strings.Replace(configured, `"Ports":{}`, `"Ports":{"5433/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]}`, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if isVerifiedTicketE2EPostgresTarget(tc.dsn, project, id, []byte(tc.inspected), []byte(network)) {
				t.Fatal("unverified or externally exposed Postgres target accepted")
			}
		})
	}
}

type bookingPostgresContainerInspect struct {
	ID    string `json:"Id"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	HostConfig struct {
		PublishAllPorts bool `json:"PublishAllPorts"`
		PortBindings    map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"PortBindings"`
	} `json:"HostConfig"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

type bookingPostgresNetworkInspect struct {
	Name     string            `json:"Name"`
	Internal bool              `json:"Internal"`
	Labels   map[string]string `json:"Labels"`
}

func isVerifiedTicketE2EPostgresTarget(dsn, project, containerID string, containerJSON, networkJSON []byte) bool {
	projectPattern := regexp.MustCompile(`^rushgo-ticket-e2e-[0-9]{8}-[0-9a-f]{12}$`)
	containerIDPattern := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if !projectPattern.MatchString(project) || !containerIDPattern.MatchString(containerID) {
		return false
	}
	u, err := url.Parse(dsn)
	if err != nil || !isIsolatedBookingTestDatabaseURL(dsn) {
		return false
	}
	var containers []bookingPostgresContainerInspect
	if json.Unmarshal(containerJSON, &containers) != nil || len(containers) != 1 {
		return false
	}
	container := containers[0]
	if container.ID != containerID || !container.State.Running {
		return false
	}
	if container.Config.Labels["com.docker.compose.project"] != project || container.Config.Labels["com.docker.compose.service"] != "ticket-e2e-postgres" || container.Config.Labels["rushgo.ticket_e2e.project"] != project {
		return false
	}
	// Linux internal bridge networks may have no active host publication.
	// Bind the private DSN to this container, while still rejecting any
	// configured or effective exposure outside the exact loopback contract.
	configured := container.HostConfig.PortBindings["5432/tcp"]
	if container.HostConfig.PublishAllPorts || len(container.HostConfig.PortBindings) != 1 ||
		len(configured) != 1 || configured[0].HostIP != "127.0.0.1" || configured[0].HostPort != "25435" {
		return false
	}
	bindings := container.NetworkSettings.Ports["5432/tcp"]
	for port, effective := range container.NetworkSettings.Ports {
		if port != "5432/tcp" || len(effective) > 1 {
			return false
		}
		if len(effective) == 1 && (effective[0].HostIP != "127.0.0.1" || effective[0].HostPort != "25435") {
			return false
		}
	}
	expectedNetwork := project + "_ticket-e2e"
	attachment, attached := container.NetworkSettings.Networks[expectedNetwork]
	// Docker Desktop does not activate a host publication for an internal-only
	// network. The explicitly owned local MVP fixture may also attach to the
	// default bridge; the exact container ID and sole loopback publication are
	// still verified above. Private-IP targets and historical fixtures retain
	// the single internal-network requirement.
	_, desktopBridge := container.NetworkSettings.Networks["bridge"]
	ownedDesktopFixture := u.Hostname() == "127.0.0.1" && desktopBridge &&
		container.Config.Labels["rushgo.owner"] == "mvp-three-rounds-20261112" &&
		strings.HasPrefix(project, "rushgo-ticket-e2e-20261112-")
	if !attached || (len(container.NetworkSettings.Networks) != 1 &&
		!(ownedDesktopFixture && len(container.NetworkSettings.Networks) == 2)) {
		return false
	}
	if u.Hostname() == "127.0.0.1" {
		if u.Port() != "25435" || len(bindings) != 1 {
			return false
		}
	} else {
		if u.Port() != "5432" || attachment.IPAddress != u.Hostname() {
			return false
		}
	}
	var networks []bookingPostgresNetworkInspect
	if json.Unmarshal(networkJSON, &networks) != nil || len(networks) != 1 {
		return false
	}
	network := networks[0]
	if network.Name != expectedNetwork || !network.Internal || network.Labels["com.docker.compose.project"] != project || network.Labels["com.docker.compose.network"] != "ticket-e2e" || network.Labels["rushgo.ticket_e2e.project"] != project {
		return false
	}
	return true
}

func requireVerifiedTicketE2EPostgres(t *testing.T, dsn string) {
	t.Helper()
	project := os.Getenv("TICKET_E2E_PROJECT")
	containerID := os.Getenv("BOOKING_TEST_DATABASE_CONTAINER_ID")
	if project == "" || containerID == "" {
		t.Fatal("BLOCKED: verified isolated Booking Postgres container identity is required")
	}
	containerJSON, err := exec.Command("docker", "inspect", containerID).Output()
	if err != nil {
		t.Fatal("BLOCKED: could not verify isolated Booking Postgres container")
	}
	networkJSON, err := exec.Command("docker", "network", "inspect", project+"_ticket-e2e").Output()
	if err != nil || !isVerifiedTicketE2EPostgresTarget(dsn, project, containerID, containerJSON, networkJSON) {
		t.Fatal("BLOCKED: Booking test database does not match the verified isolated Postgres container and internal network")
	}
}

func TestMVPDesktopFixtureRequiresExactOwnerAndLoopbackPublication(t *testing.T) {
	project := "rushgo-ticket-e2e-20261112-0123456789ab"
	id := strings.Repeat("a", 64)
	container := `[{"Id":"` + id + `","State":{"Running":true},"Config":{"Labels":{"com.docker.compose.project":"` + project + `","com.docker.compose.service":"ticket-e2e-postgres","rushgo.ticket_e2e.project":"` + project + `","rushgo.owner":"mvp-three-rounds-20261112"}},"HostConfig":{"PortBindings":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]}},"NetworkSettings":{"Ports":{"5432/tcp":[{"HostIp":"127.0.0.1","HostPort":"25435"}]},"Networks":{"` + project + `_ticket-e2e":{"IPAddress":"172.28.0.2"},"bridge":{"IPAddress":"172.17.0.3"}}}}]`
	network := `[{"Name":"` + project + `_ticket-e2e","Internal":true,"Labels":{"com.docker.compose.project":"` + project + `","com.docker.compose.network":"ticket-e2e","rushgo.ticket_e2e.project":"` + project + `"}}]`
	dsn := "postgres://reliability_fixture@127.0.0.1:25435/ticketbox_booking_test?sslmode=disable"
	if !isVerifiedTicketE2EPostgresTarget(dsn, project, id, []byte(container), []byte(network)) {
		t.Fatal("owned loopback-only Desktop fixture rejected")
	}
	for _, bad := range []string{strings.ReplaceAll(container, "mvp-three-rounds-20261112", "unowned"), strings.ReplaceAll(container, "127.0.0.1", "0.0.0.0"), strings.Replace(container, `"Running":true`, `"Running":false`, 1)} {
		if isVerifiedTicketE2EPostgresTarget(dsn, project, id, []byte(bad), []byte(network)) {
			t.Fatal("unowned, public or stopped Desktop fixture accepted")
		}
	}
	if isVerifiedTicketE2EPostgresTarget(strings.Replace(dsn, "127.0.0.1:25435", "172.28.0.2:5432", 1), project, id, []byte(container), []byte(network)) {
		t.Fatal("private target with extra bridge accepted")
	}
}
