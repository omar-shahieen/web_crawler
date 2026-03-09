import SearchBar from '../components/SearchBar.jsx'
import styles from './Home.module.css'

/**
 * Home — the landing page.
 * Features a vertically & horizontally centered search bar,
 * a branded logo, and quick-link suggestions.
 */
function Home() {
  return (
    <main className={styles.page}>
      {/* Decorative background orb */}
      <div className={styles.orb} aria-hidden="true" />

      <div className={styles.center}>
        {/* Logo / Brand */}
        <div className={styles.logoWrap}>
          <span className={styles.logoIcon} aria-hidden="true">✦</span>
          <h1 className={styles.logo}>Nexus</h1>
        </div>

        <p className={styles.tagline}>Search with clarity.</p>

        {/* Search bar */}
        <SearchBar />

        {/* Hint */}
        <p className={styles.hint}>Press Enter or click Search to explore</p>
      </div>

      {/* Footer */}
      <footer className={styles.footer}>
        <span>© 2025 Nexus Search</span>
      </footer>
    </main>
  )
}

export default Home
