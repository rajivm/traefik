package integration

import (
	"fmt"
	"net/http"
	"time"

	"github.com/containous/traefik/integration/try"
	marathon "github.com/gambol99/go-marathon"
	"github.com/go-check/check"
	checker "github.com/vdemeester/shakers"
)

// Marathon test suites (using libcompose)
type MarathonSuite struct{ BaseSuite }

func (s *MarathonSuite) SetUpSuite(c *check.C) {
	fmt.Println("setting up marathon compose file")
	s.createComposeProject(c, "marathon")
	s.composeProject.Start(c)

	// FIXME Doesn't work...
	//// "github.com/gambol99/go-marathon"
	//config := marathon.NewDefaultConfig()
	//
	//marathonClient, err := marathon.NewClient(config)
	//if err != nil {
	//	c.Fatalf("Error creating Marathon client. %v", err)
	//}
	//
	//// Wait for Marathon to elect itself leader
	//err = try.Do(30*time.Second, func() error {
	//	leader, err := marathonClient.Leader()
	//
	//	if err != nil || len(leader) == 0 {
	//		return fmt.Errorf("Leader not found. %v", err)
	//	}
	//
	//	return nil
	//})
	//
	//c.Assert(err, checker.IsNil)
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
	cmd, _ := s.cmdTraefik(withConfigFile("fixtures/marathon/with-entrypoint.toml"))
	err := cmd.Start()
	c.Assert(err, checker.IsNil)
	defer cmd.Process.Kill()

	marathonURL := "http://127.0.0.1:8080"
	fmt.Printf("polling Marathon URL %s for availability\n", marathonURL)
	// wait for marathon
	err = try.GetRequest(fmt.Sprintf("%s/ping", marathonURL), 5*time.Minute, try.StatusCodeIs(http.StatusOK))
	c.Assert(err, checker.IsNil)

	// Prepare Marathon client.
	config := marathon.NewDefaultConfig()
	config.URL = marathonURL
	client, err := marathon.NewClient(config)
	c.Assert(err, checker.IsNil)

	// Deploy test application via Marathon.
	app := marathon.NewDockerApplication().Name("/whoami").CPU(0.1).Memory(32)
	app.Container.Docker.Container("emilevauge/whoami")

	fmt.Println("deploying test application")
	deployID, err := client.UpdateApplication(app, false)
	c.Assert(err, checker.IsNil)
	c.Assert(client.WaitOnDeployment(deployID.DeploymentID, 30*time.Second), checker.IsNil)

	fmt.Println("done.")
}
