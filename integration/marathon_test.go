package integration

import (
	"fmt"
	"net/http"
	"time"

	"github.com/containous/traefik/integration/try"
	"github.com/containous/traefik/types"
	marathon "github.com/gambol99/go-marathon"
	"github.com/go-check/check"
	checker "github.com/vdemeester/shakers"
)

// Marathon test suites (using libcompose)
type MarathonSuite struct{ BaseSuite }

func (s *MarathonSuite) SetUpSuite(c *check.C) {
	s.createComposeProject(c, "marathon")
	s.composeProject.Start(c)
}

func (s *MarathonSuite) TestSimpleConfiguration(c *check.C) {
	cmd, _ := s.cmdTraefik(withConfigFile("fixtures/marathon/simple.toml"))
	err := cmd.Start()
	c.Assert(err, checker.IsNil)
	defer cmd.Process.Kill()

	// TODO validate : run on 80
	// Expected a 404 as we did not configure anything
	err = try.GetRequest("http://127.0.0.1:8000/", 500*time.Millisecond, try.StatusCodeIs(http.StatusNotFound))
	c.Assert(err, checker.IsNil)
}

func (s *MarathonSuite) TestConfigurationUpdate(c *check.C) {
	cmd, output := s.cmdTraefik(withConfigFile("fixtures/marathon/simple.toml"))
	err := cmd.Start()
	c.Assert(err, checker.IsNil)
	defer cmd.Process.Kill()

	marathonURL := "http://127.0.0.1:8080"

	// Prepare Marathon client.
	config := marathon.NewDefaultConfig()
	config.URL = marathonURL
	client, err := marathon.NewClient(config)
	c.Assert(err, checker.IsNil)

	showTraefikLog := true
	defer func() {
		if showTraefikLog {
			s.displayTraefikLog(c, output)
		}
	}()

	fmt.Println("Waiting for Marathon to become ready")
	err = try.Do(1*time.Minute, func() error {
		_, err := client.Ping()
		return err
	})
	c.Assert(err, checker.IsNil)

	// Deploy test application via Marathon.
	app := marathon.NewDockerApplication().
		Name("/whoami").
		CPU(0.1).
		Memory(32).
		AddLabel(types.LabelFrontendRule, "PathPrefix:/service")
	app.Container.Docker.Container("emilevauge/whoami")

	fmt.Println("Deploying test application")
	deploy, err := client.UpdateApplication(app, false)
	c.Assert(err, checker.IsNil)
	fmt.Println("Waiting for Deployment to complete")
	c.Assert(client.WaitOnDeployment(deploy.DeploymentID, 30*time.Second), checker.IsNil)

	fmt.Println("Querying application via Traefik")
	err = try.GetRequest("http://127.0.0.1:8000/service", 5*time.Second, try.StatusCodeIs(http.StatusOK))
	c.Assert(err, checker.IsNil)
	showTraefikLog = false
}
