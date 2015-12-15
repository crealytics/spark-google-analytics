credentials in ThisBuild ++= {
  (for {
    username <- sys.env.get("SONATYPE_USERNAME")
    password <- sys.env.get("SONATYPE_PASSWORD")
  } yield {
    Credentials("Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      username,
      password)
  }).toSeq
}

pgpPassphrase in Global := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)
