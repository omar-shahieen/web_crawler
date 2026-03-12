import SearchBar from '../components/SearchBar.jsx'

import styles from './Home.module.css'


function Home() {
  return (
    <main className={styles.page}>
      <div className={styles.orb} aria-hidden="true" />

      <div className={styles.center}>
        <div className={styles.logoWrap}>
          <span className={styles.logoIcon} aria-hidden="true">✦</span>
          <h1 className={styles.logo}>Nexus</h1>
        </div>

        <p className={styles.tagline}>Search with clarity.</p>

        <SearchBar />

        <p className={styles.hint}>Press Enter or click Search to explore</p>
      </div>

      <footer className={styles.footer}>
        <span>© 2025 Nexus Search</span>
      </footer>
    </main>
  )
}


export default Home